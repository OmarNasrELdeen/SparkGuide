"""
Spark Monitoring, Debugging, and Performance Tuning
This module provides tools for monitoring Spark applications and debugging performance issues
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import json

class SparkMonitoringDebugger:
    def __init__(self, app_name="SparkMonitoring"):
        """Initialize Spark with monitoring and debugging configurations"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "/tmp/spark-events") \
            .config("spark.history.ui.port", "18080") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

    def performance_profiling(self, df, operation_name="DataFrame Operation"):
        """Profile DataFrame operations for performance analysis"""
        print(f"\n=== Performance Profiling: {operation_name} ===")

        # Execution time measurement
        start_time = time.time()

        # Get execution plan
        print("Logical Plan:")
        df.explain(True)

        # Physical plan analysis
        print("\nPhysical Plan:")
        df.explain("formatted")

        # Trigger execution and measure time
        result_count = df.count()
        execution_time = time.time() - start_time

        # Partition analysis
        partition_sizes = df.rdd.glom().map(len).collect()

        # Performance metrics
        metrics = {
            "operation": operation_name,
            "execution_time": execution_time,
            "row_count": result_count,
            "partition_count": len(partition_sizes),
            "min_partition_size": min(partition_sizes) if partition_sizes else 0,
            "max_partition_size": max(partition_sizes) if partition_sizes else 0,
            "avg_partition_size": sum(partition_sizes) / len(partition_sizes) if partition_sizes else 0,
            "throughput_rows_per_sec": result_count / execution_time if execution_time > 0 else 0
        }

        print(f"Performance Metrics:")
        for key, value in metrics.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2f}")
            else:
                print(f"  {key}: {value}")

        return metrics

    def analyze_query_execution(self, df, query_name="Query"):
        """Analyze query execution patterns and bottlenecks"""
        print(f"\n=== Query Execution Analysis: {query_name} ===")

        # Adaptive Query Execution analysis
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.logLevel", "INFO")

        # Create temporary view for SQL analysis
        df.createOrReplaceTempView("temp_analysis_table")

        # Analyze different query patterns
        queries = {
            "full_scan": "SELECT COUNT(*) FROM temp_analysis_table",
            "filtered_scan": "SELECT COUNT(*) FROM temp_analysis_table WHERE amount > 100",
            "aggregation": "SELECT category, SUM(amount) FROM temp_analysis_table GROUP BY category",
            "window_function": """
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) as rn
                FROM temp_analysis_table
            """
        }

        query_metrics = {}
        for query_type, sql in queries.items():
            start_time = time.time()
            result_df = self.spark.sql(sql)

            # Explain query
            print(f"\n{query_type.upper()} Query Plan:")
            result_df.explain("cost")

            # Execute and measure
            count = result_df.count() if "COUNT" in sql else len(result_df.collect())
            execution_time = time.time() - start_time

            query_metrics[query_type] = {
                "execution_time": execution_time,
                "result_count": count
            }

        return query_metrics

    def monitor_resource_usage(self):
        """Monitor Spark cluster resource usage"""
        print("\n=== Resource Usage Monitoring ===")

        # Get Spark context and status tracker
        sc = self.spark.sparkContext
        status_tracker = sc.statusTracker()

        # Application information
        app_info = status_tracker.getApplicationInfo()
        print(f"Application ID: {app_info.appId}")
        print(f"Application Name: {app_info.appName}")
        print(f"Application Start Time: {app_info.startTime}")

        # Executor information
        executor_infos = status_tracker.getExecutorInfos()
        total_cores = 0
        total_memory = 0
        total_memory_used = 0

        print(f"\nExecutor Information ({len(executor_infos)} executors):")
        for executor in executor_infos:
            total_cores += executor.totalCores
            total_memory += executor.maxMemory
            total_memory_used += executor.memoryUsed

            print(f"  Executor {executor.executorId}:")
            print(f"    Cores: {executor.totalCores}")
            print(f"    Memory: {executor.maxMemory / (1024**3):.2f} GB")
            print(f"    Memory Used: {executor.memoryUsed / (1024**3):.2f} GB")
            print(f"    Active Tasks: {executor.activeTasks}")
            print(f"    Failed Tasks: {executor.failedTasks}")
            print(f"    Total Tasks: {executor.totalTasks}")

        # Cluster summary
        memory_utilization = (total_memory_used / total_memory * 100) if total_memory > 0 else 0

        cluster_metrics = {
            "total_cores": total_cores,
            "total_memory_gb": total_memory / (1024**3),
            "total_memory_used_gb": total_memory_used / (1024**3),
            "memory_utilization_percent": memory_utilization,
            "executor_count": len(executor_infos)
        }

        print(f"\nCluster Summary:")
        for key, value in cluster_metrics.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2f}")
            else:
                print(f"  {key}: {value}")

        return cluster_metrics

    def debug_data_skew(self, df, partition_column):
        """Debug and identify data skew issues"""
        print(f"\n=== Data Skew Analysis for '{partition_column}' ===")

        # Analyze partition distribution
        partition_stats = df \
            .groupBy(partition_column) \
            .count() \
            .orderBy(desc("count"))

        print("Top 10 partition sizes:")
        partition_stats.show(10)

        # Statistical analysis of skew
        stats = partition_stats.agg(
            min("count").alias("min_size"),
            max("count").alias("max_size"),
            avg("count").alias("avg_size"),
            stddev("count").alias("stddev_size")
        ).collect()[0]

        skew_ratio = stats["max_size"] / stats["avg_size"] if stats["avg_size"] > 0 else 0
        cv = stats["stddev_size"] / stats["avg_size"] if stats["avg_size"] > 0 else 0

        print(f"Skew Analysis:")
        print(f"  Min partition size: {stats['min_size']}")
        print(f"  Max partition size: {stats['max_size']}")
        print(f"  Average partition size: {stats['avg_size']:.2f}")
        print(f"  Standard deviation: {stats['stddev_size']:.2f}")
        print(f"  Skew ratio (max/avg): {skew_ratio:.2f}")
        print(f"  Coefficient of variation: {cv:.2f}")

        # Skew recommendations
        if skew_ratio > 3:
            print("HIGH SKEW DETECTED! Recommendations:")
            print("  1. Consider salting the skewed keys")
            print("  2. Use different partitioning strategy")
            print("  3. Filter out or handle skewed values separately")
        elif skew_ratio > 2:
            print("MODERATE SKEW detected. Monitor performance.")
        else:
            print("Skew levels are acceptable.")

        return {
            "skew_ratio": skew_ratio,
            "coefficient_variation": cv,
            "min_size": stats["min_size"],
            "max_size": stats["max_size"],
            "avg_size": stats["avg_size"]
        }

    def bottleneck_identification(self, df, operations):
        """Identify performance bottlenecks in ETL pipeline"""
        print("\n=== Bottleneck Identification ===")

        bottleneck_results = {}

        for operation_name, operation_func in operations.items():
            print(f"\nAnalyzing: {operation_name}")

            # Measure operation performance
            start_time = time.time()
            result_df = operation_func(df)

            # Force execution
            if hasattr(result_df, 'count'):
                count = result_df.count()
            else:
                count = len(result_df.collect()) if hasattr(result_df, 'collect') else 0

            execution_time = time.time() - start_time

            # Analyze execution plan
            if hasattr(result_df, 'explain'):
                print(f"Execution plan for {operation_name}:")
                result_df.explain("simple")

            bottleneck_results[operation_name] = {
                "execution_time": execution_time,
                "result_count": count,
                "throughput": count / execution_time if execution_time > 0 else 0
            }

        # Identify slowest operations
        sorted_operations = sorted(bottleneck_results.items(),
                                 key=lambda x: x[1]["execution_time"],
                                 reverse=True)

        print(f"\nBottleneck Analysis (slowest first):")
        for operation, metrics in sorted_operations:
            print(f"  {operation}: {metrics['execution_time']:.2f}s "
                  f"({metrics['throughput']:.0f} rows/sec)")

        return bottleneck_results

    def memory_leak_detection(self, df_operations, iterations=5):
        """Detect potential memory leaks in iterative operations"""
        print(f"\n=== Memory Leak Detection ({iterations} iterations) ===")

        sc = self.spark.sparkContext
        memory_usage = []

        for i in range(iterations):
            # Get baseline memory
            executor_infos = sc.statusTracker().getExecutorInfos()
            total_memory_used = sum(e.memoryUsed for e in executor_infos)

            # Perform operations
            for operation_name, operation_func in df_operations.items():
                temp_df = operation_func()
                if hasattr(temp_df, 'count'):
                    _ = temp_df.count()  # Force execution

                # Clean up
                if hasattr(temp_df, 'unpersist'):
                    temp_df.unpersist()

            # Measure memory after operations
            executor_infos = sc.statusTracker().getExecutorInfos()
            memory_after = sum(e.memoryUsed for e in executor_infos)

            memory_usage.append({
                "iteration": i + 1,
                "memory_before": total_memory_used / (1024**2),  # MB
                "memory_after": memory_after / (1024**2),  # MB
                "memory_delta": (memory_after - total_memory_used) / (1024**2)  # MB
            })

            print(f"Iteration {i + 1}: "
                  f"Memory delta: {memory_usage[-1]['memory_delta']:.2f} MB")

            # Small delay between iterations
            time.sleep(1)

        # Analyze trend
        total_delta = memory_usage[-1]["memory_after"] - memory_usage[0]["memory_before"]
        avg_delta_per_iteration = sum(m["memory_delta"] for m in memory_usage) / iterations

        print(f"\nMemory Leak Analysis:")
        print(f"  Total memory change: {total_delta:.2f} MB")
        print(f"  Average change per iteration: {avg_delta_per_iteration:.2f} MB")

        if avg_delta_per_iteration > 100:  # 100MB threshold
            print("  WARNING: Potential memory leak detected!")
        elif avg_delta_per_iteration > 50:
            print("  CAUTION: Monitor memory usage")
        else:
            print("  Memory usage appears stable")

        return memory_usage

    def generate_performance_report(self, app_metrics):
        """Generate comprehensive performance report"""
        print("\n=== Performance Report ===")

        report = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "application_name": self.spark.sparkContext.appName,
            "metrics": app_metrics
        }

        # Save report as JSON
        report_path = f"/tmp/spark_performance_report_{int(time.time())}.json"
        try:
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            print(f"Performance report saved to: {report_path}")
        except Exception as e:
            print(f"Could not save report: {e}")

        # Print summary
        print(f"Performance Summary:")
        print(f"  Application: {report['application_name']}")
        print(f"  Report Time: {report['timestamp']}")

        return report

# Example usage
if __name__ == "__main__":
    monitor = SparkMonitoringDebugger()

    # Create sample data for testing
    data = [(i, f"category_{i % 10}", i * 10, f"2024-{(i%12)+1:02d}-01")
            for i in range(100000)]
    df = monitor.spark.createDataFrame(data, ["id", "category", "amount", "date"])

    # Performance profiling
    metrics = monitor.performance_profiling(df, "Sample DataFrame Count")

    # Resource monitoring
    cluster_metrics = monitor.monitor_resource_usage()

    # Data skew analysis
    skew_analysis = monitor.debug_data_skew(df, "category")

    # Define test operations for bottleneck analysis
    operations = {
        "filter_operation": lambda x: x.filter(col("amount") > 500),
        "groupby_operation": lambda x: x.groupBy("category").sum("amount"),
        "join_operation": lambda x: x.alias("a").join(x.alias("b"), "category")
    }

    # Bottleneck identification
    bottlenecks = monitor.bottleneck_identification(df, operations)

    # Generate comprehensive report
    all_metrics = {
        "performance": metrics,
        "cluster": cluster_metrics,
        "skew_analysis": skew_analysis,
        "bottlenecks": bottlenecks
    }

    report = monitor.generate_performance_report(all_metrics)

    monitor.spark.stop()
