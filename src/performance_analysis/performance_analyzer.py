"""
Performance Analysis and Benchmarking Module
Advanced performance analysis for Spark ETL operations
"""

import time
import psutil
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import json
from pyspark.sql import SparkSession

class PerformanceAnalyzer:
    def __init__(self, spark_session):
        """Initialize performance analyzer"""
        self.spark = spark_session
        self.metrics_history = []

    def analyze_execution_plan(self, df, operation_name):
        """Analyze Spark execution plan for optimization opportunities"""
        print(f"\n=== Execution Plan Analysis: {operation_name} ===")

        # Get logical plan
        logical_plan = df._jdf.queryExecution().logical().toString()

        # Get physical plan
        physical_plan = df._jdf.queryExecution().executedPlan().toString()

        # Get optimized plan
        optimized_plan = df._jdf.queryExecution().optimizedPlan().toString()

        analysis = {
            'operation': operation_name,
            'logical_plan': logical_plan,
            'physical_plan': physical_plan,
            'optimized_plan': optimized_plan,
            'timestamp': datetime.now().isoformat()
        }

        # Look for optimization opportunities
        optimizations = []
        if "Exchange" in physical_plan:
            optimizations.append("Consider reducing shuffles by pre-partitioning data")
        if "Sort" in physical_plan:
            optimizations.append("Consider if sorting is necessary or can be optimized")
        if "BroadcastHashJoin" not in physical_plan and "Join" in physical_plan:
            optimizations.append("Consider broadcast joins for smaller tables")

        analysis['optimization_suggestions'] = optimizations

        print(f"Optimization suggestions: {optimizations}")
        return analysis

    def benchmark_transformations(self, df, transformations):
        """Benchmark different transformation strategies"""
        results = {}

        for name, transformation_func in transformations.items():
            print(f"\nBenchmarking: {name}")

            # Measure transformation
            start_time = time.time()
            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1024 / 1024

            try:
                result_df = transformation_func(df)
                # Trigger action to measure actual execution
                count = result_df.count()

                end_time = time.time()
                final_memory = process.memory_info().rss / 1024 / 1024

                execution_time = (end_time - start_time) * 1000
                memory_usage = final_memory - initial_memory

                results[name] = {
                    'execution_time_ms': execution_time,
                    'memory_usage_mb': memory_usage,
                    'result_count': count,
                    'throughput': count / (execution_time / 1000) if execution_time > 0 else 0,
                    'success': True
                }

                print(f"  ✅ Time: {execution_time:.2f}ms, Memory: {memory_usage:.2f}MB, Count: {count}")

            except Exception as e:
                results[name] = {
                    'execution_time_ms': 0,
                    'memory_usage_mb': 0,
                    'result_count': 0,
                    'throughput': 0,
                    'success': False,
                    'error': str(e)
                }
                print(f"  ❌ Failed: {str(e)}")

        return results

    def analyze_data_skew(self, df, column):
        """Analyze data skew in specified column"""
        print(f"\n=== Data Skew Analysis: {column} ===")

        # Get value distribution
        distribution = df.groupBy(column).count().orderBy("count", ascending=False)

        # Collect statistics
        stats = distribution.agg(
            avg("count").alias("avg_count"),
            stddev("count").alias("stddev_count"),
            max("count").alias("max_count"),
            min("count").alias("min_count")
        ).collect()[0]

        # Calculate skew metrics
        skew_ratio = stats['max_count'] / stats['avg_count'] if stats['avg_count'] > 0 else 0
        cv = stats['stddev_count'] / stats['avg_count'] if stats['avg_count'] > 0 else 0

        skew_analysis = {
            'column': column,
            'skew_ratio': skew_ratio,
            'coefficient_variation': cv,
            'max_count': stats['max_count'],
            'min_count': stats['min_count'],
            'avg_count': stats['avg_count'],
            'stddev_count': stats['stddev_count']
        }

        # Determine skew level
        if skew_ratio > 10:
            skew_level = "HIGH"
            recommendations = [
                "Consider salting technique for heavy hitters",
                "Use separate processing for skewed keys",
                "Consider broadcast joins if appropriate"
            ]
        elif skew_ratio > 3:
            skew_level = "MEDIUM"
            recommendations = [
                "Monitor for performance issues",
                "Consider repartitioning strategies"
            ]
        else:
            skew_level = "LOW"
            recommendations = ["No immediate action needed"]

        skew_analysis['skew_level'] = skew_level
        skew_analysis['recommendations'] = recommendations

        print(f"Skew Level: {skew_level}")
        print(f"Skew Ratio: {skew_ratio:.2f}")
        print(f"Recommendations: {recommendations}")

        return skew_analysis

    def memory_usage_analysis(self, df_operations):
        """Analyze memory usage patterns across operations"""
        print("\n=== Memory Usage Analysis ===")

        memory_profile = []
        process = psutil.Process()

        for operation_name, operation_func in df_operations.items():
            initial_memory = process.memory_info().rss / 1024 / 1024

            try:
                start_time = time.time()
                result = operation_func()
                end_time = time.time()

                final_memory = process.memory_info().rss / 1024 / 1024

                memory_profile.append({
                    'operation': operation_name,
                    'initial_memory_mb': initial_memory,
                    'final_memory_mb': final_memory,
                    'memory_delta_mb': final_memory - initial_memory,
                    'execution_time_ms': (end_time - start_time) * 1000,
                    'success': True
                })

            except Exception as e:
                memory_profile.append({
                    'operation': operation_name,
                    'initial_memory_mb': initial_memory,
                    'final_memory_mb': initial_memory,
                    'memory_delta_mb': 0,
                    'execution_time_ms': 0,
                    'success': False,
                    'error': str(e)
                })

        # Identify memory-intensive operations
        memory_intensive = [op for op in memory_profile if op.get('memory_delta_mb', 0) > 100]

        analysis = {
            'memory_profile': memory_profile,
            'memory_intensive_operations': memory_intensive,
            'total_memory_growth': sum(op.get('memory_delta_mb', 0) for op in memory_profile)
        }

        print(f"Memory-intensive operations: {len(memory_intensive)}")
        for op in memory_intensive:
            print(f"  {op['operation']}: {op['memory_delta_mb']:.2f} MB")

        return analysis

    def scalability_analysis(self, operation_func, data_sizes):
        """Analyze how operations scale with data size"""
        print("\n=== Scalability Analysis ===")

        scalability_results = []

        for size in data_sizes:
            print(f"Testing with {size:,} records...")

            start_time = time.time()
            try:
                result = operation_func(size)
                execution_time = (time.time() - start_time) * 1000

                scalability_results.append({
                    'data_size': size,
                    'execution_time_ms': execution_time,
                    'throughput': size / (execution_time / 1000) if execution_time > 0 else 0,
                    'success': True
                })

            except Exception as e:
                scalability_results.append({
                    'data_size': size,
                    'execution_time_ms': 0,
                    'throughput': 0,
                    'success': False,
                    'error': str(e)
                })

        # Calculate scalability metrics
        successful_results = [r for r in scalability_results if r['success']]
        if len(successful_results) >= 2:
            # Linear regression to find scaling pattern
            sizes = [r['data_size'] for r in successful_results]
            times = [r['execution_time_ms'] for r in successful_results]

            # Simple linear scaling analysis
            scaling_factor = times[-1] / times[0] if times[0] > 0 else 0
            data_factor = sizes[-1] / sizes[0] if sizes[0] > 0 else 0

            if scaling_factor <= data_factor * 1.2:
                scaling_assessment = "LINEAR"
            elif scaling_factor <= data_factor * 2:
                scaling_assessment = "SLIGHTLY_SUPERLINEAR"
            else:
                scaling_assessment = "SUPERLINEAR"
        else:
            scaling_assessment = "INSUFFICIENT_DATA"

        analysis = {
            'results': scalability_results,
            'scaling_assessment': scaling_assessment,
            'successful_tests': len(successful_results),
            'total_tests': len(scalability_results)
        }

        print(f"Scaling Assessment: {scaling_assessment}")
        return analysis

    def generate_performance_visualization(self, results, output_file="performance_chart.png"):
        """Generate performance visualization charts"""
        try:
            import matplotlib.pyplot as plt

            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

            # Chart 1: Execution Time by Operation
            if 'benchmark_results' in results:
                operations = list(results['benchmark_results'].keys())
                times = [results['benchmark_results'][op]['execution_time_ms'] for op in operations]

                ax1.bar(operations, times)
                ax1.set_title('Execution Time by Operation')
                ax1.set_ylabel('Time (ms)')
                plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')

            # Chart 2: Memory Usage by Operation
            if 'memory_analysis' in results:
                memory_data = results['memory_analysis']['memory_profile']
                operations = [op['operation'] for op in memory_data if op['success']]
                memory_usage = [op['memory_delta_mb'] for op in memory_data if op['success']]

                ax2.bar(operations, memory_usage)
                ax2.set_title('Memory Usage by Operation')
                ax2.set_ylabel('Memory (MB)')
                plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')

            # Chart 3: Scalability Analysis
            if 'scalability_analysis' in results:
                scale_data = results['scalability_analysis']['results']
                sizes = [r['data_size'] for r in scale_data if r['success']]
                times = [r['execution_time_ms'] for r in scale_data if r['success']]

                ax3.plot(sizes, times, marker='o')
                ax3.set_title('Scalability: Data Size vs Execution Time')
                ax3.set_xlabel('Data Size (records)')
                ax3.set_ylabel('Execution Time (ms)')

            # Chart 4: Throughput Analysis
            if 'scalability_analysis' in results:
                throughputs = [r['throughput'] for r in scale_data if r['success']]

                ax4.plot(sizes, throughputs, marker='s', color='green')
                ax4.set_title('Throughput vs Data Size')
                ax4.set_xlabel('Data Size (records)')
                ax4.set_ylabel('Throughput (records/sec)')

            plt.tight_layout()
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            print(f"📊 Performance visualization saved to: {output_file}")

        except ImportError:
            print("⚠️  matplotlib not available, skipping visualization")
        except Exception as e:
            print(f"⚠️  Visualization failed: {e}")

# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("PerformanceAnalysis").getOrCreate()
    analyzer = PerformanceAnalyzer(spark)

    # Example performance analysis
    print("Performance Analyzer initialized successfully!")
