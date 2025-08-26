"""
Spark Parallelism and Performance Optimization
This module demonstrates how to ensure proper parallelism and optimize Spark performance
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

class SparkParallelismOptimizer:
    def __init__(self, app_name="SparkParallelism"):
        """Initialize Spark with optimized parallelism settings"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.default.parallelism", "200") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()

    def check_current_parallelism(self, df):
        """Check current parallelism settings and DataFrame partitions"""
        print(f"Default Parallelism: {self.spark.sparkContext.defaultParallelism}")
        print(f"DataFrame Partitions: {df.rdd.getNumPartitions()}")
        print(f"Shuffle Partitions: {self.spark.conf.get('spark.sql.shuffle.partitions')}")

        # Show partition distribution
        partition_counts = df.rdd.glom().map(len).collect()
        print(f"Records per partition: {partition_counts}")
        return partition_counts

    def optimize_partitioning_for_read(self, server, database, username, password, table_name, partition_column, lower_bound, upper_bound, num_partitions):
        """Read data with optimized partitioning for parallel extraction"""
        url = f"jdbc:sqlserver://{server}:1433;databaseName={database}"
        properties = {
            "user": username,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        # Read with partitioning for parallel processing
        df = self.spark.read \
            .jdbc(url=url,
                  table=table_name,
                  column=partition_column,
                  lowerBound=lower_bound,
                  upperBound=upper_bound,
                  numPartitions=num_partitions,
                  properties=properties)

        print(f"Data loaded with {num_partitions} partitions")
        self.check_current_parallelism(df)
        return df

    def demonstrate_broadcast_join(self, large_df, small_df):
        """Demonstrate broadcast join for performance optimization"""
        from pyspark.sql.functions import broadcast

        # Regular join
        start_time = time.time()
        regular_join = large_df.join(small_df, "common_column")
        regular_count = regular_join.count()
        regular_time = time.time() - start_time

        # Broadcast join
        start_time = time.time()
        broadcast_join = large_df.join(broadcast(small_df), "common_column")
        broadcast_count = broadcast_join.count()
        broadcast_time = time.time() - start_time

        print(f"Regular join time: {regular_time:.2f}s, Count: {regular_count}")
        print(f"Broadcast join time: {broadcast_time:.2f}s, Count: {broadcast_count}")

        return broadcast_join

    def optimize_shuffle_operations(self, df):
        """Optimize operations that cause shuffles"""
        # Cache frequently used DataFrames
        df.cache()

        # Use reduceByKey instead of groupByKey when possible
        # Example: instead of df.groupBy("col").agg(sum("value"))
        # Use: df.rdd.map(lambda x: (x.col, x.value)).reduceByKey(lambda a, b: a + b)

        # Minimize shuffles by combining operations
        optimized_df = df \
            .filter(col("status") == "active") \
            .select("id", "name", "value") \
            .groupBy("name") \
            .agg(sum("value").alias("total_value")) \
            .orderBy("total_value")

        return optimized_df

    def memory_optimization_techniques(self, df):
        """Demonstrate memory optimization techniques"""
        # Persist with appropriate storage level
        from pyspark import StorageLevel

        # For memory-rich clusters
        df.persist(StorageLevel.MEMORY_AND_DISK_SER)

        # For memory-constrained clusters
        # df.persist(StorageLevel.DISK_ONLY)

        # Unpersist when no longer needed
        # df.unpersist()

        # Use columnar storage for better compression
        # This is automatically used with Parquet format

        return df

    def parallel_write_optimization(self, df, output_path, file_format="parquet"):
        """Optimize parallel writes"""
        # Repartition before writing for optimal file sizes
        # Target: 128MB - 1GB per file
        num_partitions = max(1, df.count() // 1000000)  # Rough estimate

        optimized_df = df.repartition(num_partitions)

        if file_format == "parquet":
            optimized_df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(output_path)
        elif file_format == "delta":
            optimized_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(output_path)

        print(f"Data written to {output_path} with {num_partitions} files")

    def analyze_parallelism_potential(self, df):
        """Analyze parallelism potential for the test suite"""
        print(f"\n=== Parallelism Analysis ===")
        print(f"Analyzing DataFrame parallelism potential")

        results = {}

        try:
            # 1. Current parallelism metrics
            current_partitions = df.rdd.getNumPartitions()
            default_parallelism = self.spark.sparkContext.defaultParallelism
            shuffle_partitions = int(self.spark.conf.get('spark.sql.shuffle.partitions'))

            # 2. Partition distribution analysis
            partition_sizes = df.rdd.glom().map(len).collect()
            total_records = sum(partition_sizes)

            # 3. Calculate parallelism metrics
            avg_partition_size = total_records / current_partitions if current_partitions > 0 else 0
            max_partition_size = max(partition_sizes) if partition_sizes else 0
            min_partition_size = min(partition_sizes) if partition_sizes else 0

            # 4. Parallelism efficiency calculation
            efficiency = min_partition_size / max_partition_size if max_partition_size > 0 else 0

            # 5. Recommendations
            optimal_partitions = max(1, total_records // 100000)  # Target ~100k records per partition
            parallelism_score = min(100, (efficiency * 100))

            results = {
                'current_partitions': current_partitions,
                'default_parallelism': default_parallelism,
                'shuffle_partitions': shuffle_partitions,
                'total_records': total_records,
                'avg_partition_size': avg_partition_size,
                'max_partition_size': max_partition_size,
                'min_partition_size': min_partition_size,
                'partition_efficiency': efficiency,
                'parallelism_score': parallelism_score,
                'recommended_partitions': optimal_partitions,
                'partition_distribution': partition_sizes
            }

            print(f"✅ Parallelism analysis completed successfully")
            print(f"   Current partitions: {current_partitions}")
            print(f"   Parallelism score: {parallelism_score:.2f}/100")
            print(f"   Partition efficiency: {efficiency:.2f}")
            print(f"   Recommended partitions: {optimal_partitions}")

            return results

        except Exception as e:
            print(f"❌ Parallelism analysis failed: {e}")
            return {'error': str(e)}

# Example usage
if __name__ == "__main__":
    optimizer = SparkParallelismOptimizer()

    # Create sample data
    data = [(i, f"name_{i}", i * 10) for i in range(1000000)]
    df = optimizer.spark.createDataFrame(data, ["id", "name", "value"])

    # Check parallelism
    optimizer.check_current_parallelism(df)

    # Optimize operations
    optimized_df = optimizer.optimize_shuffle_operations(df)
    optimized_df.show(10)

    optimizer.spark.stop()
