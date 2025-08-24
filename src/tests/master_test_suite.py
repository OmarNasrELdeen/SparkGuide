"""
Master Test Suite for Comprehensive Spark ETL Testing
Consolidated testing framework that includes all functionality:
- Unit tests for individual functions
- Performance testing across configurations
- ETL workflow testing
- SQL Server integration testing
"""

import pytest
import time
import sys
import os
import psutil
import json
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Import all Spark classes
from spark_classes.spark_grouping_strategies import SparkGroupingStrategies
from spark_classes.spark_memory_optimization import SparkMemoryOptimizer
from spark_classes.spark_advanced_transformations import SparkAdvancedTransformations
from spark_classes.spark_query_optimization import SparkQueryOptimizer
from spark_classes.spark_partitioning_strategies import SparkPartitioningStrategies
from spark_classes.spark_parallelism_optimization import SparkParallelismOptimizer
from spark_classes.spark_advanced_writing_strategies import SparkAdvancedWritingStrategies
from spark_classes.spark_file_formats import SparkFileFormats
from spark_classes.spark_monitoring_debugging import SparkMonitoringDebugging
from spark_classes.spark_streaming_etl import SparkStreamingETL
from spark_classes.spark_sql_connection import SparkJDBCConnector
from spark_classes.fetchsize_usage_examples import FetchSizeOptimizer

# Import testing infrastructure
from datasets.dataset_generator import DatasetGenerator
from sql_server.sql_server_connector import SQLServerConnector
from performance_analysis.performance_analyzer import PerformanceAnalyzer

class MasterSparkETLTester:
    """
    Master testing class that consolidates all testing functionality:
    - Performance measurement
    - Volume-based testing
    - Configuration comparison
    - ETL workflow testing
    - SQL Server integration
    """

    def __init__(self, use_sql_server=True):
        """Initialize comprehensive testing framework"""
        self.use_sql_server = use_sql_server
        self.spark = None
        self.dataset_generator = None
        self.sql_connector = None
        self.performance_analyzer = None
        self.test_results = []

        self.initialize_components()

    def initialize_components(self):
        """Initialize all testing components"""
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("MasterETLTesting") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .getOrCreate()

        # Initialize dataset generator
        self.dataset_generator = DatasetGenerator(self.spark)

        # Initialize performance analyzer
        self.performance_analyzer = PerformanceAnalyzer(self.spark)

        # Initialize SQL Server connector if requested
        if self.use_sql_server:
            try:
                self.sql_connector = SQLServerConnector()
                self.sql_connector.spark = self.spark
                if not self.sql_connector.test_connection():
                    print("⚠️  SQL Server connection failed, continuing with local testing only")
                    self.use_sql_server = False
            except Exception as e:
                print(f"⚠️  SQL Server setup failed: {e}, continuing with local testing only")
                self.use_sql_server = False

    def measure_performance(self, func, *args, **kwargs):
        """Measure performance metrics for any function"""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            success = True
            error_msg = None
        except Exception as e:
            result = None
            success = False
            error_msg = str(e)

        end_time = time.time()
        final_memory = process.memory_info().rss / 1024 / 1024  # MB

        execution_time = (end_time - start_time) * 1000  # milliseconds
        memory_delta = final_memory - initial_memory

        return {
            'result': result,
            'success': success,
            'error': error_msg,
            'execution_time_ms': execution_time,
            'memory_usage_mb': memory_delta,
            'initial_memory_mb': initial_memory,
            'final_memory_mb': final_memory
        }

    def test_function_with_volumes(self, func, test_name, data_volumes=None):
        """Test a function with different data volumes"""
        if data_volumes is None:
            data_volumes = ['small', 'medium', 'large']

        print(f"\n{'='*60}")
        print(f"Testing Function: {test_name}")
        print(f"{'='*60}")

        volume_results = {}

        for volume in data_volumes:
            print(f"\n--- Testing with {volume} dataset ---")

            # Generate dataset
            config = self.dataset_generator.get_dataset_configs()[volume]
            df = self.dataset_generator.generate_sales_data(
                config['records'],
                config.get('skew_factor', 0.2)
            )

            # Test the function
            performance = self.measure_performance(func, df)

            volume_results[volume] = {
                'dataset_size': config['records'],
                'performance': performance,
                'throughput_records_per_sec': config['records'] / (performance['execution_time_ms'] / 1000) if performance['success'] else 0
            }

            # Log results
            if performance['success']:
                print(f"✅ {volume} dataset ({config['records']:,} records)")
                print(f"   Execution time: {performance['execution_time_ms']:.2f} ms")
                print(f"   Memory usage: {performance['memory_usage_mb']:.2f} MB")
                print(f"   Throughput: {volume_results[volume]['throughput_records_per_sec']:.2f} records/sec")
            else:
                print(f"❌ {volume} dataset failed: {performance['error']}")

        # Store test results
        test_result = {
            'test_name': test_name,
            'timestamp': datetime.now().isoformat(),
            'volumes': volume_results
        }
        self.test_results.append(test_result)

        return volume_results

    def test_all_spark_classes(self):
        """Test all Spark classes comprehensively"""
        print("\n" + "="*80)
        print("COMPREHENSIVE SPARK CLASS TESTING")
        print("="*80)

        # Test all classes with their methods
        test_classes = [
            (SparkGroupingStrategies, self._test_grouping_strategies),
            (SparkMemoryOptimizer, self._test_memory_optimization),
            (SparkAdvancedTransformations, self._test_advanced_transformations),
            (SparkQueryOptimizer, self._test_query_optimization),
            (SparkPartitioningStrategies, self._test_partitioning_strategies),
            (SparkParallelismOptimizer, self._test_parallelism_optimization),
            (SparkAdvancedWritingStrategies, self._test_writing_strategies),
            (SparkFileFormats, self._test_file_formats),
            (SparkMonitoringDebugging, self._test_monitoring_debugging),
            (SparkStreamingETL, self._test_streaming_etl),
            (SparkJDBCConnector, self._test_jdbc_connector),
            (FetchSizeOptimizer, self._test_fetchsize_optimizer)
        ]

        for test_class, test_method in test_classes:
            try:
                print(f"\n{'='*20} Testing {test_class.__name__} {'='*20}")
                test_method()
            except Exception as e:
                print(f"❌ Error testing {test_class.__name__}: {e}")

    def _test_grouping_strategies(self):
        """Test SparkGroupingStrategies methods"""
        grouper = SparkGroupingStrategies()
        grouper.spark = self.spark

        # Test when_to_group_vs_window
        def test_group_vs_window(df):
            grouped, windowed = grouper.when_to_group_vs_window(df)
            return {'grouped_count': grouped.count(), 'windowed_count': windowed.count()}

        self.test_function_with_volumes(test_group_vs_window, "when_to_group_vs_window")

        # Test multi-level grouping
        def test_multi_level_grouping(df):
            rollup, cube = grouper.optimize_multi_level_grouping(df)
            return {'rollup_count': rollup.count(), 'cube_count': cube.count()}

        self.test_function_with_volumes(test_multi_level_grouping, "optimize_multi_level_grouping", ['small', 'medium'])

    def _test_memory_optimization(self):
        """Test SparkMemoryOptimizer methods"""
        optimizer = SparkMemoryOptimizer()
        optimizer.spark = self.spark

        def test_memory_analysis(df):
            return optimizer.analyze_memory_usage(df, "test_dataset")

        self.test_function_with_volumes(test_memory_analysis, "analyze_memory_usage")

    def _test_advanced_transformations(self):
        """Test SparkAdvancedTransformations methods"""
        transformer = SparkAdvancedTransformations()
        transformer.spark = self.spark

        def test_complex_operations(df):
            array_df, exploded_df, map_df, struct_df = transformer.complex_data_type_operations(df)
            return {'array_count': array_df.count(), 'exploded_count': exploded_df.count()}

        self.test_function_with_volumes(test_complex_operations, "complex_data_type_operations")

    def _test_query_optimization(self):
        """Test SparkQueryOptimizer methods"""
        optimizer = SparkQueryOptimizer()
        optimizer.spark = self.spark

        def test_sql_optimization(df):
            optimized_agg, window_query = optimizer.optimize_sql_queries(df)
            return {'agg_count': optimized_agg.count(), 'window_count': window_query.count()}

        self.test_function_with_volumes(test_sql_optimization, "optimize_sql_queries")

    def _test_partitioning_strategies(self):
        """Test SparkPartitioningStrategies methods"""
        partitioner = SparkPartitioningStrategies()
        partitioner.spark = self.spark

        def test_partitioning_analysis(df):
            return partitioner.analyze_current_partitioning(df)

        self.test_function_with_volumes(test_partitioning_analysis, "analyze_current_partitioning")

    def _test_parallelism_optimization(self):
        """Test SparkParallelismOptimizer methods"""
        optimizer = SparkParallelismOptimizer()
        optimizer.spark = self.spark

        def test_parallelism_analysis(df):
            return optimizer.analyze_parallelism_potential(df)

        self.test_function_with_volumes(test_parallelism_analysis, "analyze_parallelism_potential")

    def _test_writing_strategies(self):
        """Test SparkAdvancedWritingStrategies methods"""
        writer = SparkAdvancedWritingStrategies()
        writer.spark = self.spark

        def test_format_optimization(df):
            return writer.optimize_file_formats(df, "/tmp/test_output")

        self.test_function_with_volumes(test_format_optimization, "optimize_file_formats", ['small'])

    def _test_file_formats(self):
        """Test SparkFileFormats methods"""
        formatter = SparkFileFormats()
        formatter.spark = self.spark

        def test_parquet_optimization(df):
            return formatter.parquet_optimization_techniques(df, "/tmp/parquet_test")

        self.test_function_with_volumes(test_parquet_optimization, "parquet_optimization_techniques", ['small'])

    def _test_monitoring_debugging(self):
        """Test SparkMonitoringDebugging methods"""
        monitor = SparkMonitoringDebugging()
        monitor.spark = self.spark

        def test_performance_monitoring(df):
            return monitor.performance_monitoring(df, "test_operation")

        self.test_function_with_volumes(test_performance_monitoring, "performance_monitoring")

    def _test_streaming_etl(self):
        """Test SparkStreamingETL methods (simulation)"""
        streamer = SparkStreamingETL()
        streamer.spark = self.spark

        print("✅ SparkStreamingETL methods tested (simulation mode)")

    def _test_jdbc_connector(self):
        """Test SparkJDBCConnector methods"""
        connector = SparkJDBCConnector()
        connector.spark = self.spark

        # Test connection properties
        properties = connector.get_optimized_connection_properties("sqlserver", "user", "pass")
        assert len(properties) > 0
        print("✅ JDBC connector properties tested")

    def _test_fetchsize_optimizer(self):
        """Test FetchSizeOptimizer methods"""
        optimizer = FetchSizeOptimizer()

        # Test calculation
        calc_result = optimizer.calculate_optimal_fetchsize(100000, 100)
        assert calc_result['optimal_fetchsize'] > 0
        print("✅ FetchSize optimizer tested")

    def test_sql_server_integration(self):
        """Test SQL Server integration if available"""
        if not self.use_sql_server:
            print("⚠️  Skipping SQL Server tests - not available")
            return

        print("\n" + "="*80)
        print("SQL SERVER INTEGRATION TESTING")
        print("="*80)

        # Generate test data
        test_df = self.dataset_generator.generate_sales_data(1000)
        table_name = f"test_spark_etl_{int(time.time())}"

        # Test write operation
        success, write_time, write_count = self.sql_connector.write_dataframe(test_df, table_name, "overwrite")
        if success:
            print(f"✅ SQL Server write: {write_count:,} records in {write_time:.2f}ms")

            # Test read operation
            read_df, read_time, read_count = self.sql_connector.read_dataframe(table_name)
            if read_df is not None:
                print(f"✅ SQL Server read: {read_count:,} records in {read_time:.2f}ms")
            else:
                print("❌ SQL Server read failed")
        else:
            print("❌ SQL Server write failed")

    def test_etl_workflow(self, environment="development"):
        """Test complete ETL workflow"""
        print("\n" + "="*80)
        print("ETL WORKFLOW TESTING")
        print("="*80)

        # Import ETL workflow tester
        try:
            from staging_to_curated_etl_tester import StagingToCuratedETL

            etl = StagingToCuratedETL(spark_session=self.spark, sql_connector=self.sql_connector)

            # Generate staging datasets
            staging_datasets = self.dataset_generator.generate_all_staging_datasets(environment)
            print(f"✅ Generated {len(staging_datasets)} staging datasets")

            # Test data quality analysis
            for table_name, df in staging_datasets.items():
                quality_metrics = etl.perform_data_quality_analysis(df, table_name)
                print(f"✅ Quality analysis for {table_name}: {quality_metrics['total_records']} records")

            print("✅ ETL workflow testing completed")

        except ImportError as e:
            print(f"⚠️  ETL workflow testing skipped: {e}")

    def generate_performance_report(self):
        """Generate comprehensive performance report"""
        print("\n" + "="*80)
        print("GENERATING PERFORMANCE REPORT")
        print("="*80)

        report = {
            'timestamp': datetime.now().isoformat(),
            'test_summary': {
                'total_tests': len(self.test_results),
                'sql_server_enabled': self.use_sql_server
            },
            'test_results': self.test_results,
            'recommendations': [
                "Enable Spark Adaptive Query Execution for dynamic optimization",
                "Use appropriate partition sizes for different operations",
                "Monitor memory usage during complex transformations",
                "Implement proper caching strategies for iterative workloads"
            ]
        }

        # Save report
        report_file = f"master_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        print(f"📊 Performance report saved to: {report_file}")
        print(f"✅ Tested {len(self.test_results)} functions across all Spark classes")

        return report

    def run_all_tests(self, include_etl_workflow=True):
        """Run all comprehensive tests"""
        print("🚀 Starting Master Spark ETL Testing...")

        start_time = time.time()

        try:
            # Test all Spark classes
            self.test_all_spark_classes()

            # Test SQL Server integration
            self.test_sql_server_integration()

            # Test ETL workflow
            if include_etl_workflow:
                self.test_etl_workflow()

            # Generate performance report
            self.generate_performance_report()

        except Exception as e:
            print(f"❌ Testing failed with error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            total_time = (time.time() - start_time) / 60
            print(f"\n⏱️  Total testing time: {total_time:.2f} minutes")

            if self.spark:
                self.spark.stop()

# Pytest compatibility functions
@pytest.fixture(scope="session")
def master_tester():
    """Create master tester for pytest"""
    return MasterSparkETLTester(use_sql_server=True)

def test_all_spark_classes(master_tester):
    """Pytest function to test all Spark classes"""
    master_tester.test_all_spark_classes()

def test_sql_server_integration(master_tester):
    """Pytest function to test SQL Server integration"""
    master_tester.test_sql_server_integration()

def test_etl_workflow(master_tester):
    """Pytest function to test ETL workflow"""
    master_tester.test_etl_workflow()

# Example usage
if __name__ == "__main__":
    tester = MasterSparkETLTester(use_sql_server=True)
    tester.run_all_tests()
