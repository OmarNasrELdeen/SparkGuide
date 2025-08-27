"""
Staging-to-Curated ETL Workflow with Performance Analysis
Comprehensive ETL pipeline testing with data quality, cleaning, and performance optimization
"""

import time
import json
import psutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from datasets.dataset_generator import DatasetGenerator
from sql_server.sql_server_connector import SQLServerConnector
from performance_analysis.performance_analyzer import PerformanceAnalyzer

class StagingToCuratedETL:
    def __init__(self, spark_session=None, sql_connector=None):
        """Initialize staging-to-curated ETL pipeline"""
        # Validate the provided spark_session; fallback to active or new session if invalid
        if spark_session is not None and hasattr(spark_session, "createDataFrame"):
            self.spark = spark_session
        else:
            active = SparkSession.getActiveSession()
            self.spark = active or self.create_optimized_spark_session()

        self.sql_connector = sql_connector or SQLServerConnector()
        self.sql_connector.spark = self.spark
        self.performance_analyzer = PerformanceAnalyzer(self.spark)
        self.etl_metrics = []

    def create_optimized_spark_session(self):
        """Create optimized Spark session for ETL operations"""
        return SparkSession.builder \
            .appName("StagingToCuratedETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

    def load_staging_data_to_sql_server(self, datasets, database_name="staging_db"):
        """Load staging datasets to SQL Server"""
        print("\n" + "="*80)
        print("LOADING STAGING DATA TO SQL SERVER")
        print("="*80)

        load_results = {}

        for table_name, df in datasets.items():
            print(f"\n--- Loading {table_name} to SQL Server ---")

            start_time = time.time()

            try:
                # Write to SQL Server staging database
                success, write_time, record_count = self.sql_connector.write_dataframe(
                    df, f"{database_name}.{table_name}", mode="overwrite"
                )

                load_results[table_name] = {
                    'success': success,
                    'write_time_ms': write_time,
                    'record_count': record_count,
                    'throughput_records_per_sec': record_count / (write_time / 1000) if write_time > 0 else 0
                }

                print(f"✅ Loaded {record_count:,} records in {write_time:.2f}ms")

            except Exception as e:
                load_results[table_name] = {
                    'success': False,
                    'error': str(e),
                    'write_time_ms': 0,
                    'record_count': 0
                }
                print(f"❌ Failed to load {table_name}: {e}")

        return load_results

    def perform_data_quality_analysis(self, df, table_name):
        """Perform comprehensive data quality analysis"""
        print(f"\n=== Data Quality Analysis: {table_name} ===")

        quality_metrics = {
            'table_name': table_name,
            'total_records': df.count(),
            'total_columns': len(df.columns),
            'null_analysis': {},
            'data_type_issues': {},
            'business_rule_violations': {},
            'duplicate_analysis': {}
        }

        # Null analysis
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = (null_count / quality_metrics['total_records']) * 100
            quality_metrics['null_analysis'][column] = {
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2)
            }

        # Duplicate analysis
        distinct_count = df.distinct().count()
        duplicate_count = quality_metrics['total_records'] - distinct_count
        quality_metrics['duplicate_analysis'] = {
            'total_duplicates': duplicate_count,
            'duplicate_percentage': round((duplicate_count / quality_metrics['total_records']) * 100, 2)
        }

        # Business rule violations (table-specific)
        if 'amount' in df.columns:
            negative_amounts = df.filter(col("amount") < 0).count()
            quality_metrics['business_rule_violations']['negative_amounts'] = negative_amounts

        if 'transaction_date' in df.columns:
            future_dates = df.filter(col("transaction_date") > current_date()).count()
            quality_metrics['business_rule_violations']['future_dates'] = future_dates

        if 'customer_id' in df.columns:
            invalid_customer_ids = df.filter(col("customer_id").isNull() | (col("customer_id") == "")).count()
            quality_metrics['business_rule_violations']['invalid_customer_ids'] = invalid_customer_ids

        print(f"Total Records: {quality_metrics['total_records']:,}")
        print(f"Duplicates: {duplicate_count:,} ({quality_metrics['duplicate_analysis']['duplicate_percentage']}%)")

        return quality_metrics

    def clean_staging_data(self, df, table_name, cleaning_config=None):
        """Clean staging data based on business rules"""
        print(f"\n=== Cleaning {table_name} ===")

        if cleaning_config is None:
            cleaning_config = self.get_default_cleaning_config(table_name)

        cleaned_df = df
        cleaning_stats = {
            'original_count': df.count(),
            'operations_applied': [],
            'records_removed': 0,
            'records_modified': 0
        }

        # Remove duplicates
        if cleaning_config.get('remove_duplicates', False):
            before_count = cleaned_df.count()
            cleaned_df = cleaned_df.distinct()
            after_count = cleaned_df.count()
            removed = before_count - after_count
            cleaning_stats['records_removed'] += removed
            cleaning_stats['operations_applied'].append(f"Removed {removed:,} duplicates")

        # Handle null values
        if cleaning_config.get('handle_nulls', {}):
            null_config = cleaning_config['handle_nulls']
            for column, strategy in null_config.items():
                if column in cleaned_df.columns:
                    if strategy == 'drop':
                        before_count = cleaned_df.count()
                        cleaned_df = cleaned_df.filter(col(column).isNotNull())
                        after_count = cleaned_df.count()
                        removed = before_count - after_count
                        cleaning_stats['records_removed'] += removed
                        cleaning_stats['operations_applied'].append(f"Dropped {removed:,} null {column} records")
                    elif strategy == 'default_value':
                        default_val = null_config.get(f"{column}_default", "Unknown")
                        cleaned_df = cleaned_df.fillna({column: default_val})
                        cleaning_stats['operations_applied'].append(f"Filled null {column} with '{default_val}'")

        # Business rule validations
        if cleaning_config.get('business_rules', {}):
            rules = cleaning_config['business_rules']

            # Remove negative amounts
            if rules.get('remove_negative_amounts', False) and 'amount' in cleaned_df.columns:
                before_count = cleaned_df.count()
                cleaned_df = cleaned_df.filter(col("amount") >= 0)
                after_count = cleaned_df.count()
                removed = before_count - after_count
                cleaning_stats['records_removed'] += removed
                cleaning_stats['operations_applied'].append(f"Removed {removed:,} negative amounts")

            # Remove future dates
            if rules.get('remove_future_dates', False) and 'transaction_date' in cleaned_df.columns:
                before_count = cleaned_df.count()
                cleaned_df = cleaned_df.filter(col("transaction_date") <= current_date())
                after_count = cleaned_df.count()
                removed = before_count - after_count
                cleaning_stats['records_removed'] += removed
                cleaning_stats['operations_applied'].append(f"Removed {removed:,} future dates")

        # Data standardization
        if cleaning_config.get('standardize', {}):
            std_config = cleaning_config['standardize']
            for column, operations in std_config.items():
                if column in cleaned_df.columns:
                    for operation in operations:
                        if operation == 'upper':
                            cleaned_df = cleaned_df.withColumn(column, upper(col(column)))
                        elif operation == 'trim':
                            cleaned_df = cleaned_df.withColumn(column, trim(col(column)))
                        elif operation == 'title':
                            cleaned_df = cleaned_df.withColumn(column, initcap(col(column)))
                    cleaning_stats['operations_applied'].append(f"Standardized {column}")

        cleaning_stats['final_count'] = cleaned_df.count()

        print(f"Cleaning completed: {cleaning_stats['original_count']:,} → {cleaning_stats['final_count']:,} records")
        for operation in cleaning_stats['operations_applied']:
            print(f"  - {operation}")

        return cleaned_df, cleaning_stats

    def get_default_cleaning_config(self, table_name):
        """Get default cleaning configuration for different table types"""
        configs = {
            'staging_transactions': {
                'remove_duplicates': True,
                'handle_nulls': {
                    'customer_id': 'drop',
                    'product_id': 'drop',
                    'amount': 'drop'
                },
                'business_rules': {
                    'remove_negative_amounts': True,
                    'remove_future_dates': True
                },
                'standardize': {
                    'channel': ['upper', 'trim'],
                    'status': ['upper', 'trim']
                }
            },
            'staging_customers': {
                'remove_duplicates': True,
                'handle_nulls': {
                    'customer_id': 'drop',
                    'customer_name': 'drop',
                    'email': 'default_value',
                    'email_default': 'no-email@unknown.com'
                },
                'standardize': {
                    'customer_name': ['title', 'trim'],
                    'email': ['trim'],
                    'status': ['upper']
                }
            },
            'staging_products': {
                'remove_duplicates': True,
                'handle_nulls': {
                    'product_id': 'drop',
                    'product_name': 'drop'
                },
                'standardize': {
                    'product_name': ['title', 'trim'],
                    'category': ['title'],
                    'brand': ['title'],
                    'status': ['upper']
                }
            }
        }

        return configs.get(table_name, {
            'remove_duplicates': True,
            'handle_nulls': {},
            'business_rules': {},
            'standardize': {}
        })

    def perform_etl_joins_and_aggregations(self, cleaned_datasets):
        """Perform complex joins and aggregations for curated data"""
        print("\n" + "="*80)
        print("PERFORMING ETL JOINS AND AGGREGATIONS")
        print("="*80)

        curated_datasets = {}
        join_metrics = {}

        # Create customer dimension (cleaned and enriched)
        print("\n--- Creating Customer Dimension ---")
        start_time = time.time()

        customer_dim = cleaned_datasets['staging_customers'] \
            .withColumn("customer_age_group",
                       when(col("created_date") < date_sub(current_date(), 1095), "Veteran")
                       .when(col("created_date") < date_sub(current_date(), 365), "Established")
                       .otherwise("New")) \
            .withColumn("risk_category",
                       when(col("credit_limit") > 50000, "Low Risk")
                       .when(col("credit_limit") > 20000, "Medium Risk")
                       .otherwise("High Risk"))

        curated_datasets['dim_customer'] = customer_dim
        join_metrics['dim_customer'] = {
            'execution_time_ms': (time.time() - start_time) * 1000,
            'record_count': customer_dim.count()
        }

        # Create product dimension with category analytics
        print("\n--- Creating Product Dimension ---")
        start_time = time.time()

        product_dim = cleaned_datasets['staging_products'] \
            .withColumn("price_tier",
                       when(col("list_price") > 1000, "Premium")
                       .when(col("list_price") > 100, "Standard")
                       .otherwise("Budget")) \
            .withColumn("margin_percentage",
                       round(((col("list_price") - col("cost_price")) / col("list_price")) * 100, 2))

        curated_datasets['dim_product'] = product_dim
        join_metrics['dim_product'] = {
            'execution_time_ms': (time.time() - start_time) * 1000,
            'record_count': product_dim.count()
        }

        # Create comprehensive sales fact table
        print("\n--- Creating Sales Fact Table ---")
        start_time = time.time()

        sales_fact = cleaned_datasets['staging_transactions'] \
            .join(customer_dim.select("customer_id", "customer_type", "region", "tier", "risk_category"),
                  "customer_id", "inner") \
            .join(product_dim.select("product_id", "category", "brand", "list_price", "cost_price", "price_tier"),
                  "product_id", "inner") \
            .join(cleaned_datasets['staging_stores'].select("store_id", "store_type", "region").withColumnRenamed("region", "store_region"),
                  "store_id", "inner") \
            .withColumn("revenue", col("amount")) \
            .withColumn("cost", col("cost_price") * col("quantity")) \
            .withColumn("profit", col("revenue") - col("cost")) \
            .withColumn("profit_margin_pct",
                       round((col("profit") / col("revenue")) * 100, 2)) \
            .withColumn("year", year(col("transaction_date"))) \
            .withColumn("month", month(col("transaction_date"))) \
            .withColumn("quarter", quarter(col("transaction_date"))) \
            .withColumn("day_of_week", dayofweek(col("transaction_date")))

        curated_datasets['fact_sales'] = sales_fact
        join_metrics['fact_sales'] = {
            'execution_time_ms': (time.time() - start_time) * 1000,
            'record_count': sales_fact.count()
        }

        # Create aggregated sales summary
        print("\n--- Creating Sales Summary Aggregations ---")
        start_time = time.time()

        sales_summary = sales_fact \
            .groupBy("year", "month", "category", "region", "customer_type") \
            .agg(
                sum("revenue").alias("total_revenue"),
                sum("profit").alias("total_profit"),
                avg("profit_margin_pct").alias("avg_profit_margin"),
                count("transaction_id").alias("transaction_count"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("product_id").alias("unique_products")
            ) \
            .withColumn("revenue_per_customer",
                       round(col("total_revenue") / col("unique_customers"), 2))

        curated_datasets['agg_sales_summary'] = sales_summary
        join_metrics['agg_sales_summary'] = {
            'execution_time_ms': (time.time() - start_time) * 1000,
            'record_count': sales_summary.count()
        }

        return curated_datasets, join_metrics

    def load_curated_data_to_sql_server(self, curated_datasets, database_name="curated_db"):
        """Load curated datasets to SQL Server"""
        print("\n" + "="*80)
        print("LOADING CURATED DATA TO SQL SERVER")
        print("="*80)

        load_results = {}

        for table_name, df in curated_datasets.items():
            print(f"\n--- Loading {table_name} to Curated Database ---")

            try:
                # Optimize DataFrame before writing
                optimized_df = df.coalesce(8)  # Optimize partition count

                success, write_time, record_count = self.sql_connector.write_dataframe(
                    optimized_df, f"{database_name}.{table_name}", mode="overwrite"
                )

                load_results[table_name] = {
                    'success': success,
                    'write_time_ms': write_time,
                    'record_count': record_count,
                    'throughput_records_per_sec': record_count / (write_time / 1000) if write_time > 0 else 0
                }

                print(f"✅ Loaded {record_count:,} records in {write_time:.2f}ms")

            except Exception as e:
                load_results[table_name] = {
                    'success': False,
                    'error': str(e),
                    'write_time_ms': 0,
                    'record_count': 0
                }
                print(f"❌ Failed to load {table_name}: {e}")

        return load_results

    def run_comprehensive_etl_with_performance_analysis(self, environment="staging",
                                                       test_configurations=None):
        """Run complete ETL pipeline with performance analysis across configurations"""
        print("\n" + "="*100)
        print("COMPREHENSIVE STAGING-TO-CURATED ETL WITH PERFORMANCE ANALYSIS")
        print("="*100)

        if test_configurations is None:
            test_configurations = self.get_etl_test_configurations()

        all_results = {}

        for config_name, config in test_configurations.items():
            print(f"\n{'='*20} Testing Configuration: {config_name} {'='*20}")

            # Create new Spark session with specific configuration
            spark = SparkSession.builder \
                .master("local[*]") \
                .appName(f"ETL_Test_{config_name}")

            for key, value in config['spark_configs'].items():
                spark = spark.config(key, value)

            test_spark = spark.getOrCreate()

            try:
                # Initialize components with test Spark session
                generator = DatasetGenerator(test_spark)
                sql_connector = SQLServerConnector()
                sql_connector.spark = test_spark

                # Generate staging datasets
                staging_datasets = generator.generate_all_staging_datasets(environment)

                # Track overall pipeline performance
                pipeline_start = time.time()

                # Step 1: Load to staging database
                staging_load_results = self.load_staging_data_to_sql_server(staging_datasets, "staging_db")

                # Step 2: Data quality analysis
                quality_results = {}
                for table_name, df in staging_datasets.items():
                    quality_results[table_name] = self.perform_data_quality_analysis(df, table_name)

                # Step 3: Data cleaning
                cleaned_datasets = {}
                cleaning_results = {}
                for table_name, df in staging_datasets.items():
                    cleaned_df, cleaning_stats = self.clean_staging_data(df, table_name)
                    cleaned_datasets[table_name] = cleaned_df
                    cleaning_results[table_name] = cleaning_stats

                # Step 4: ETL joins and aggregations
                curated_datasets, join_metrics = self.perform_etl_joins_and_aggregations(cleaned_datasets)

                # Step 5: Load to curated database
                curated_load_results = self.load_curated_data_to_sql_server(curated_datasets, "curated_db")

                pipeline_total_time = (time.time() - pipeline_start) * 1000

                # Compile results
                config_results = {
                    'configuration': config,
                    'pipeline_total_time_ms': pipeline_total_time,
                    'staging_load_results': staging_load_results,
                    'quality_results': quality_results,
                    'cleaning_results': cleaning_results,
                    'join_metrics': join_metrics,
                    'curated_load_results': curated_load_results,
                    'total_records_processed': sum(result.get('record_count', 0)
                                                 for result in staging_load_results.values()),
                    'overall_throughput': sum(result.get('record_count', 0)
                                            for result in staging_load_results.values()) / (pipeline_total_time / 1000)
                }

                all_results[config_name] = config_results

                print(f"\n📊 Configuration {config_name} Results:")
                print(f"  Total Pipeline Time: {pipeline_total_time:.2f}ms")
                print(f"  Records Processed: {config_results['total_records_processed']:,}")
                print(f"  Overall Throughput: {config_results['overall_throughput']:.2f} records/sec")

            finally:
                test_spark.stop()

        # Generate comprehensive comparison report
        comparison_report = self.generate_etl_performance_report(all_results)

        return all_results, comparison_report

    def get_etl_test_configurations(self):
        """Define different Spark configurations for ETL testing"""
        return {
            "baseline": {
                "description": "Baseline configuration",
                "spark_configs": {
                    "spark.sql.adaptive.enabled": "false"
                }
            },
            "adaptive_enabled": {
                "description": "Adaptive Query Execution enabled",
                "spark_configs": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.sql.adaptive.skewJoin.enabled": "true"
                }
            },
            "memory_optimized": {
                "description": "Memory optimized for large joins",
                "spark_configs": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.executor.memory": "4g",
                    "spark.executor.memoryFraction": "0.8",
                    "spark.sql.execution.arrow.pyspark.enabled": "true"
                }
            },
            "join_optimized": {
                "description": "Optimized for complex joins",
                "spark_configs": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.skewJoin.enabled": "true",
                    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "536870912",
                    "spark.sql.autoBroadcastJoinThreshold": "104857600"
                }
            },
            "io_optimized": {
                "description": "Optimized for I/O operations",
                "spark_configs": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.files.maxPartitionBytes": "134217728",
                    "spark.sql.parquet.compression.codec": "snappy",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                }
            }
        }

    def generate_etl_performance_report(self, all_results):
        """Generate comprehensive ETL performance comparison report"""
        print("\n" + "="*80)
        print("ETL PERFORMANCE COMPARISON REPORT")
        print("="*80)

        report = {
            'timestamp': datetime.now().isoformat(),
            'configurations_tested': len(all_results),
            'configuration_comparison': {},
            'best_performers': {},
            'recommendations': []
        }

        # Find best performers for each metric
        metrics = ['pipeline_total_time_ms', 'overall_throughput', 'total_records_processed']

        for metric in metrics:
            if metric == 'pipeline_total_time_ms':
                # Lower is better
                best_config = min(all_results.keys(),
                                key=lambda k: all_results[k].get(metric, float('inf')))
            else:
                # Higher is better
                best_config = max(all_results.keys(),
                                key=lambda k: all_results[k].get(metric, 0))

            report['best_performers'][metric] = {
                'configuration': best_config,
                'value': all_results[best_config].get(metric, 0)
            }

        # Configuration comparison
        for config_name, results in all_results.items():
            report['configuration_comparison'][config_name] = {
                'pipeline_time_ms': results.get('pipeline_total_time_ms', 0),
                'throughput_records_per_sec': results.get('overall_throughput', 0),
                'total_records': results.get('total_records_processed', 0),
                'avg_staging_load_time': sum(r.get('write_time_ms', 0)
                                           for r in results.get('staging_load_results', {}).values()) /
                                          len(results.get('staging_load_results', {})) if results.get('staging_load_results') else 0,
                'avg_join_time': sum(r.get('execution_time_ms', 0)
                                   for r in results.get('join_metrics', {}).values()) /
                                len(results.get('join_metrics', {})) if results.get('join_metrics') else 0
            }

        # Generate recommendations
        recommendations = []

        fastest_config = report['best_performers']['pipeline_total_time_ms']['configuration']
        highest_throughput = report['best_performers']['overall_throughput']['configuration']

        recommendations.append(f"Fastest overall pipeline: {fastest_config}")
        recommendations.append(f"Highest throughput: {highest_throughput}")

        if 'adaptive' in fastest_config:
            recommendations.append("Adaptive Query Execution significantly improves ETL performance")

        if 'memory' in highest_throughput:
            recommendations.append("Memory optimization is crucial for high-throughput ETL")

        recommendations.extend([
            "Enable adaptive features for dynamic optimization during ETL",
            "Use appropriate partition sizes for different data volumes",
            "Implement data quality checks early in the pipeline",
            "Optimize join strategies based on data characteristics",
            "Monitor memory usage during complex transformations"
        ])

        report['recommendations'] = recommendations

        # Save detailed report
        report_file = f"etl_performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        print(f"\n📊 ETL performance report saved to: {report_file}")

        # Print summary
        print(f"\n=== PERFORMANCE SUMMARY ===")
        print(f"Configurations tested: {len(all_results)}")
        print(f"Fastest pipeline: {fastest_config}")
        print(f"Highest throughput: {highest_throughput}")

        for config_name, comparison in report['configuration_comparison'].items():
            print(f"\n{config_name}:")
            print(f"  Pipeline time: {comparison['pipeline_time_ms']:.2f}ms")
            print(f"  Throughput: {comparison['throughput_records_per_sec']:.2f} records/sec")

        return report

# Example usage
if __name__ == "__main__":
    etl = StagingToCuratedETL()

    # Run comprehensive ETL testing
    results, report = etl.run_comprehensive_etl_with_performance_analysis(
        environment="staging"
    )

    print("\n✅ Comprehensive ETL testing completed!")
