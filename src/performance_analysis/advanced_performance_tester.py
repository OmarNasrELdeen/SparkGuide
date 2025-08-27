"""
Advanced Performance Testing with Different Spark Configurations
Tests performance across various Spark configurations, partitioning strategies, and data volumes
"""

import time
import json
import psutil
import builtins
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Import all Spark classes for testing
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from spark_classes.spark_grouping_strategies import SparkGroupingStrategies
from spark_classes.spark_memory_optimization import SparkMemoryOptimizer
from spark_classes.spark_partitioning_strategies import SparkPartitioningStrategies
from spark_classes.spark_parallelism_optimization import SparkParallelismOptimizer
from spark_classes.spark_query_optimization import SparkQueryOptimizer
from datasets.dataset_generator import DatasetGenerator

class AdvancedPerformanceTester:
    def __init__(self):
        """Initialize advanced performance tester"""
        self.test_results = []
        self.configuration_results = {}

    def get_spark_configurations(self):
        """Define different Spark configurations for testing"""
        return {
            "default": {
                "name": "Default Configuration",
                "configs": {}
            },
            "adaptive_optimized": {
                "name": "Adaptive Query Execution Optimized",
                "configs": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.sql.adaptive.skewJoin.enabled": "true",
                    "spark.sql.adaptive.localShuffleReader.enabled": "true",
                    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728"  # 128MB
                }
            },
            "memory_optimized": {
                "name": "Memory Optimized",
                "configs": {
                    "spark.executor.memory": "4g",
                    "spark.executor.memoryFraction": "0.8",
                    "spark.executor.memoryStorageFraction": "0.5",
                    "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                }
            },
            "large_data_optimized": {
                "name": "Large Data Optimized",
                "configs": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.sql.files.maxPartitionBytes": "268435456",  # 256MB
                    "spark.sql.shuffle.partitions": "400",
                    "spark.executor.cores": "4",
                    "spark.executor.memory": "6g"
                }
            },
            "join_optimized": {
                "name": "Join Optimized",
                "configs": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.skewJoin.enabled": "true",
                    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "536870912",  # 512MB
                    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
                    "spark.sql.autoBroadcastJoinThreshold": "104857600"  # 100MB
                }
            },
            "io_optimized": {
                "name": "I/O Optimized",
                "configs": {
                    "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
                    "spark.sql.files.openCostInBytes": "4194304",  # 4MB
                    "spark.sql.parquet.compression.codec": "snappy",
                    "spark.sql.orc.compression.codec": "zlib"
                }
            }
        }

    def create_spark_session(self, config_name, config):
        """Create Spark session with specific configuration"""
        builder = SparkSession.builder \
            .master("local[*]") \
            .appName(f"PerformanceTest_{config_name}")

        for key, value in config.items():
            builder = builder.config(key, value)

        return builder.getOrCreate()

    def measure_class_performance(self, spark_class, method_name, df, config_name):
        """Measure performance of a specific class method"""
        start_time = time.time()
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        try:
            # Execute the method
            if hasattr(spark_class, method_name):
                method = getattr(spark_class, method_name)
                if callable(method):
                    if method_name in ['when_to_group_vs_window', 'optimize_multi_level_grouping']:
                        result = method(df)
                    elif method_name in ['handle_skewed_grouping']:
                        result = method(df, "category")
                    elif method_name in ['optimize_grouping_for_time_series']:
                        result = method(df, "date")
                    else:
                        result = method(df)

                    # Trigger action if result is DataFrame
                    if hasattr(result, 'count'):
                        count = result.count()
                    elif isinstance(result, (list, tuple)):
                        count = sum(r.count() if hasattr(r, 'count') else 1 for r in result)
                    else:
                        count = 1
                else:
                    count = 0
                    result = None
            else:
                count = 0
                result = None

            success = True

        except Exception as e:
            success = False
            count = 0
            result = None
            error_msg = str(e)

        end_time = time.time()
        final_memory = process.memory_info().rss / 1024 / 1024  # MB

        execution_time = (end_time - start_time) * 1000  # milliseconds
        memory_delta = final_memory - initial_memory

        return {
            'config_name': config_name,
            'class_name': spark_class.__class__.__name__,
            'method_name': method_name,
            'success': success,
            'execution_time_ms': execution_time,
            'memory_delta_mb': memory_delta,
            'initial_memory_mb': initial_memory,
            'final_memory_mb': final_memory,
            'result_count': count,
            'throughput_records_per_sec': (count / (execution_time / 1000)) if execution_time > 0 and count > 0 else 0
        }

    def test_all_classes_with_configurations(self, dataset_sizes=['small', 'medium']):
        """Test all Spark classes with different configurations"""
        print("\n" + "="*80)
        print("COMPREHENSIVE PERFORMANCE TESTING WITH DIFFERENT CONFIGURATIONS")
        print("="*80)

        configurations = self.get_spark_configurations()

        for config_name, config_info in configurations.items():
            print(f"\n{'='*20} Testing {config_info['name']} {'='*20}")

            spark = self.create_spark_session(config_name, config_info['configs'])

            try:
                # Generate test data
                generator = DatasetGenerator(spark)

                config_results = {}

                for size in dataset_sizes:
                    print(f"\n--- Testing with {size} dataset ---")

                    # Get dataset configuration
                    dataset_config = generator.get_dataset_configs()[size]
                    df = generator.generate_sales_data(
                        dataset_config['records'],
                        dataset_config.get('skew_factor', 0.2)
                    )

                    size_results = {}

                    # Test Grouping Strategies
                    grouper = SparkGroupingStrategies()
                    grouper.spark = spark

                    grouping_methods = [
                        'when_to_group_vs_window',
                        'optimize_multi_level_grouping',
                        'handle_skewed_grouping',
                        'optimize_grouping_for_time_series'
                    ]

                    for method in grouping_methods:
                        result = self.measure_class_performance(grouper, method, df, config_name)
                        size_results[f"grouping_{method}"] = result

                    # Test Memory Optimization
                    memory_optimizer = SparkMemoryOptimizer()
                    memory_optimizer.spark = spark

                    memory_methods = [
                        'analyze_memory_usage',
                        'optimize_caching_strategies',
                        'memory_efficient_operations'
                    ]

                    for method in memory_methods:
                        result = self.measure_class_performance(memory_optimizer, method, df, config_name)
                        size_results[f"memory_{method}"] = result

                    # Test Partitioning Strategies
                    partitioner = SparkPartitioningStrategies()
                    partitioner.spark = spark

                    partitioning_methods = [
                        'analyze_current_partitioning',
                        'optimal_partitioning_strategies',
                        'coalesce_vs_repartition'
                    ]

                    for method in partitioning_methods:
                        result = self.measure_class_performance(partitioner, method, df, config_name)
                        size_results[f"partitioning_{method}"] = result

                    config_results[size] = size_results

                self.configuration_results[config_name] = config_results

            finally:
                spark.stop()

        return self.configuration_results

    def generate_configuration_comparison_report(self):
        """Generate detailed comparison report across configurations"""
        print("\n" + "="*80)
        print("CONFIGURATION PERFORMANCE COMPARISON REPORT")
        print("="*80)

        report = {
            'timestamp': datetime.now().isoformat(),
            'configuration_results': self.configuration_results,
            'summary': {},
            'recommendations': []
        }

        # Analyze best performing configurations for each operation
        operation_performance = {}

        for config_name, config_data in self.configuration_results.items():
            for size, size_data in config_data.items():
                for operation, metrics in size_data.items():
                    if operation not in operation_performance:
                        operation_performance[operation] = {}

                    if size not in operation_performance[operation]:
                        operation_performance[operation][size] = {}

                    operation_performance[operation][size][config_name] = {
                        'execution_time_ms': metrics['execution_time_ms'],
                        'memory_delta_mb': metrics['memory_delta_mb'],
                        'throughput': metrics['throughput_records_per_sec']
                    }

        # Find best configurations for each operation
        best_configs = {}
        for operation, size_data in operation_performance.items():
            best_configs[operation] = {}
            for size, config_data in size_data.items():
                # Find fastest configuration using builtins.min to avoid shadowing
                fastest_config = builtins.min(config_data.keys(),
                                   key=lambda k: config_data[k]['execution_time_ms'])
                best_configs[operation][size] = fastest_config

        report['best_configurations'] = best_configs

        # Generate recommendations
        recommendations = []

        # Analyze patterns
        adaptive_wins = sum(1 for op_data in best_configs.values()
                          for config in op_data.values()
                          if 'adaptive' in config)

        memory_wins = sum(1 for op_data in best_configs.values()
                         for config in op_data.values()
                         if 'memory' in config)

        if adaptive_wins > len(best_configs) * 0.3:
            recommendations.append("Adaptive Query Execution shows significant benefits across operations")

        if memory_wins > len(best_configs) * 0.3:
            recommendations.append("Memory optimization is crucial for performance")

        recommendations.extend([
            "Use different configurations based on workload characteristics",
            "Monitor memory usage and adjust executor memory accordingly",
            "Enable adaptive features for dynamic optimization",
            "Consider data size when choosing partition strategies"
        ])

        report['recommendations'] = recommendations

        # Save detailed report
        report_file = f"configuration_performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        print(f"\n📊 Configuration comparison report saved to: {report_file}")

        # Print summary
        print(f"\n=== PERFORMANCE SUMMARY ===")
        for operation, size_data in best_configs.items():
            print(f"\n{operation}:")
            for size, best_config in size_data.items():
                execution_time = operation_performance[operation][size][best_config]['execution_time_ms']
                print(f"  {size}: {best_config} ({execution_time:.2f}ms)")

        return report

    def benchmark_scaling_performance(self, scaling_sizes=[1000, 10000, 100000, 500000]):
        """Benchmark how performance scales with data size"""
        print("\n" + "="*80)
        print("SCALING PERFORMANCE ANALYSIS")
        print("="*80)

        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("ScalingTest") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()

        try:
            generator = DatasetGenerator(spark)
            scaling_results = {}

            for size in scaling_sizes:
                print(f"\n--- Testing with {size:,} records ---")

                df = generator.generate_sales_data(size)

                # Test key operations
                operations = {
                    'simple_groupby': lambda d: d.groupBy("category").sum("sales").count(),
                    'complex_aggregation': lambda d: d.groupBy("category", "region").agg(
                        sum("sales"), avg("sales"), count("*")
                    ).count(),
                    'window_function': lambda d: d.withColumn(
                        "rank", dense_rank().over(Window.partitionBy("category").orderBy(desc("sales")))
                    ).count()
                }

                size_results = {}

                for op_name, op_func in operations.items():
                    start_time = time.time()
                    try:
                        result = op_func(df)
                        execution_time = (time.time() - start_time) * 1000
                        throughput = size / (execution_time / 1000) if execution_time > 0 else 0

                        size_results[op_name] = {
                            'execution_time_ms': execution_time,
                            'throughput': throughput,
                            'success': True
                        }
                    except Exception as e:
                        size_results[op_name] = {
                            'execution_time_ms': 0,
                            'throughput': 0,
                            'success': False,
                            'error': str(e)
                        }

                scaling_results[size] = size_results

            # Analyze scaling patterns
            scaling_analysis = self.analyze_scaling_patterns(scaling_results)

            print(f"\n=== SCALING ANALYSIS ===")
            for operation, analysis in scaling_analysis.items():
                print(f"{operation}: {analysis['scaling_type']} scaling")
                print(f"  Performance factor: {analysis['performance_factor']:.2f}x")

            return scaling_results, scaling_analysis

        finally:
            spark.stop()

    def analyze_scaling_patterns(self, scaling_results):
        """Analyze scaling patterns from results"""
        operations = list(next(iter(scaling_results.values())).keys())
        scaling_analysis = {}

        for operation in operations:
            sizes = sorted(scaling_results.keys())
            times = [scaling_results[size][operation]['execution_time_ms']
                    for size in sizes if scaling_results[size][operation]['success']]

            if len(times) >= 2:
                # Simple analysis: compare first and last
                time_factor = times[-1] / times[0] if times[0] > 0 else 0
                size_factor = sizes[-1] / sizes[0] if sizes[0] > 0 else 0

                if time_factor <= size_factor * 1.2:
                    scaling_type = "LINEAR"
                elif time_factor <= size_factor * 2:
                    scaling_type = "SLIGHTLY_SUPERLINEAR"
                else:
                    scaling_type = "SUPERLINEAR"

                scaling_analysis[operation] = {
                    'scaling_type': scaling_type,
                    'performance_factor': time_factor / size_factor if size_factor > 0 else 0
                }
            else:
                scaling_analysis[operation] = {
                    'scaling_type': "INSUFFICIENT_DATA",
                    'performance_factor': 0
                }

        return scaling_analysis

# Example usage
if __name__ == "__main__":
    tester = AdvancedPerformanceTester()

    # Test all configurations
    results = tester.test_all_classes_with_configurations(['small', 'medium'])

    # Generate comparison report
    report = tester.generate_configuration_comparison_report()

    # Test scaling performance
    scaling_results, scaling_analysis = tester.benchmark_scaling_performance()

    print("\n✅ Advanced performance testing completed!")
