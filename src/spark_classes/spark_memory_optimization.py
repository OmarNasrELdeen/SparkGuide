"""
Spark Memory Management and Performance Tuning
This module demonstrates memory optimization techniques for large-scale ETL processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel

class SparkMemoryOptimizer:
    def __init__(self, app_name="SparkMemoryOptimizer"):
        """Initialize Spark with memory optimization configurations"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.memoryFraction", "0.8") \
            .config("spark.executor.memoryStorageFraction", "0.5") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryo.registrationRequired", "false") \
            .getOrCreate()

    def analyze_memory_usage(self, df, description="DataFrame"):
        """Analyze memory usage and storage levels"""
        print(f"\n=== Memory Analysis for {description} ===")

        # Check if DataFrame is cached
        storage_level = df.storageLevel
        print(f"Storage Level: {storage_level}")
        print(f"Is Cached: {storage_level != StorageLevel.NONE}")

        # Estimate DataFrame size
        row_count = df.count()
        num_partitions = df.rdd.getNumPartitions()
        print(f"Rows: {row_count:,}")
        print(f"Partitions: {num_partitions}")
        print(f"Avg rows per partition: {row_count // num_partitions if num_partitions > 0 else 0:,}")

        # Memory usage estimation (rough)
        estimated_size_mb = (row_count * len(df.columns) * 8) / (1024 * 1024)  # Rough estimate
        print(f"Estimated size: {estimated_size_mb:.2f} MB")

        return {
            "row_count": row_count,
            "partitions": num_partitions,
            "estimated_size_mb": estimated_size_mb,
            "is_cached": storage_level != StorageLevel.NONE
        }

    def optimize_caching_strategies(self, df):
        """Demonstrate different caching strategies"""
        print("\n=== Caching Strategy Optimization ===")

        # Strategy 1: Memory-only caching (fastest, but risky)
        memory_only_df = df.persist(StorageLevel.MEMORY_ONLY)
        print("MEMORY_ONLY: Fastest access, but data lost if executor fails")

        # Strategy 2: Memory and disk caching (balanced)
        memory_disk_df = df.persist(StorageLevel.MEMORY_AND_DISK)
        print("MEMORY_AND_DISK: Balanced performance and fault tolerance")

        # Strategy 3: Serialized memory caching (memory efficient)
        memory_ser_df = df.persist(StorageLevel.MEMORY_ONLY_SER)
        print("MEMORY_ONLY_SER: More memory efficient but slower serialization")

        # Strategy 4: Disk-only caching (memory constrained environments)
        disk_only_df = df.persist(StorageLevel.DISK_ONLY)
        print("DISK_ONLY: For memory-constrained clusters")

        # Strategy 5: Replicated caching (high availability)
        replicated_df = df.persist(StorageLevel.MEMORY_AND_DISK_2)
        print("MEMORY_AND_DISK_2: Replicated for high availability")

        return {
            "memory_only": memory_only_df,
            "memory_disk": memory_disk_df,
            "memory_ser": memory_ser_df,
            "disk_only": disk_only_df,
            "replicated": replicated_df
        }

    def memory_efficient_operations(self, large_df):
        """Demonstrate memory-efficient operations for large datasets"""
        print("\n=== Memory-Efficient Operations ===")

        # 1. Use iterative processing for very large datasets
        def process_in_chunks(df, chunk_size=100000):
            total_rows = df.count()
            chunks = []

            for i in range(0, total_rows, chunk_size):
                chunk = df.filter((col("id") >= i) & (col("id") < i + chunk_size))
                processed_chunk = chunk.groupBy("category").sum("amount")
                chunks.append(processed_chunk)

            # Union all chunks
            result = chunks[0]
            for chunk in chunks[1:]:
                result = result.union(chunk)

            return result.groupBy("category").sum("sum(amount)")

        # 2. Use sampling for exploratory analysis
        sample_df = large_df.sample(0.1, seed=42)  # 10% sample
        sample_stats = sample_df.describe()
        print("Use sampling for initial data exploration")

        # 3. Columnar operations instead of row-wise
        # Efficient: vectorized operations
        efficient_df = large_df \
            .select("category", "amount") \
            .groupBy("category") \
            .agg(sum("amount").alias("total"), avg("amount").alias("average"))

        # 4. Avoid wide transformations when possible
        # Prefer narrow transformations (filter, select, map)
        narrow_ops = large_df \
            .filter(col("amount") > 100) \
            .select("category", "amount", "date") \
            .withColumn("amount_category",
                       when(col("amount") > 1000, "high")
                       .when(col("amount") > 500, "medium")
                       .otherwise("low"))

        return sample_df, efficient_df, narrow_ops

    def garbage_collection_optimization(self, df):
        """Optimize garbage collection for long-running jobs"""
        print("\n=== Garbage Collection Optimization ===")

        # Process data in batches to control memory usage
        def batch_process_with_gc(df, batch_size=50000):
            total_count = df.count()
            results = []

            for i in range(0, total_count, batch_size):
                # Process batch
                batch = df.filter((col("id") >= i) & (col("id") < i + batch_size))
                batch_result = batch.groupBy("category").sum("amount")

                # Collect result to driver (small data)
                batch_data = batch_result.collect()
                results.extend(batch_data)

                # Unpersist batch to free memory
                batch.unpersist()

                # Force garbage collection (in real scenarios, let JVM handle this)
                if i % (batch_size * 10) == 0:
                    print(f"Processed {i + batch_size} rows...")

            return self.spark.createDataFrame(results, batch_result.schema)

        # Cache management
        def managed_cache_operations(df):
            # Cache DataFrame
            df.cache()

            # Perform operations
            result1 = df.filter(col("amount") > 100).count()
            result2 = df.groupBy("category").sum("amount")

            # Unpersist when done
            df.unpersist()

            return result1, result2

        return batch_process_with_gc, managed_cache_operations

    def spill_and_shuffle_optimization(self, df1, df2):
        """Optimize operations that cause memory spills and shuffles"""
        print("\n=== Spill and Shuffle Optimization ===")

        # 1. Optimize joins to reduce shuffles
        # Broadcast small tables
        if df2.count() < 1000000:  # Small table threshold
            broadcast_join = df1.join(broadcast(df2), "key")
            print("Used broadcast join for small table")
        else:
            # For large tables, partition by join key first
            partitioned_df1 = df1.repartition(col("key"))
            partitioned_df2 = df2.repartition(col("key"))
            regular_join = partitioned_df1.join(partitioned_df2, "key")
            print("Pre-partitioned large tables for join")

        # 2. Optimize aggregations
        # Use map-side pre-aggregation when possible
        pre_aggregated = df1 \
            .repartition(col("category")) \
            .groupBy("category", "subcategory") \
            .sum("amount")

        final_aggregated = pre_aggregated \
            .groupBy("category") \
            .sum("sum(amount)")

        # 3. Optimize sort operations
        # Use repartitionByRange for ordered data
        sorted_df = df1.repartitionByRange(10, col("date"))

        print("Optimized shuffles through partitioning and pre-aggregation")

        return pre_aggregated, final_aggregated, sorted_df

    def monitor_memory_metrics(self):
        """Monitor Spark memory metrics"""
        print("\n=== Memory Monitoring ===")

        # Get Spark context for metrics
        sc = self.spark.sparkContext

        # Executor memory info
        executor_infos = sc.statusTracker().getExecutorInfos()

        print("Executor Memory Information:")
        for executor in executor_infos:
            print(f"Executor {executor.executorId}: "
                  f"Max Memory: {executor.maxMemory / (1024**3):.2f} GB, "
                  f"Memory Used: {executor.memoryUsed / (1024**3):.2f} GB")

        # Application metrics
        app_status = sc.statusTracker().getApplicationInfo()
        print(f"Application: {app_status.appName}")
        print(f"Start Time: {app_status.startTime}")

        # Stage metrics (if any stages are running)
        active_stages = sc.statusTracker().getActiveStageIds()
        print(f"Active Stages: {len(active_stages)}")

        return executor_infos, app_status

    def memory_leak_prevention(self, df):
        """Prevent memory leaks in long-running applications"""
        print("\n=== Memory Leak Prevention ===")

        # 1. Proper cache management
        cached_dfs = []

        def safe_cache_operation(dataframe, operation_name):
            # Cache DataFrame
            cached_df = dataframe.cache()
            cached_dfs.append(cached_df)

            # Perform operation
            result = cached_df.groupBy("category").sum("amount")

            print(f"Completed {operation_name}")
            return result

        # 2. Cleanup function
        def cleanup_cached_dataframes():
            for cached_df in cached_dfs:
                cached_df.unpersist()
            cached_dfs.clear()
            print("Cleaned up all cached DataFrames")

        # 3. Limited lifetime operations
        def limited_lifetime_operation(df, max_iterations=10):
            for i in range(max_iterations):
                temp_df = df.filter(col("id") % 10 == i)
                result = temp_df.count()
                # Don't accumulate references
                del temp_df

                if i % 3 == 0:  # Periodic cleanup
                    print(f"Iteration {i}, count: {result}")

        return safe_cache_operation, cleanup_cached_dataframes, limited_lifetime_operation

# Example usage
if __name__ == "__main__":
    optimizer = SparkMemoryOptimizer()

    # Create large sample dataset
    data = [(i, f"category_{i % 20}", f"subcategory_{i % 5}", i * 10, f"2024-{(i%12)+1:02d}-01")
            for i in range(500000)]
    large_df = optimizer.spark.createDataFrame(data,
        ["id", "category", "subcategory", "amount", "date"])

    # Analyze memory usage
    stats = optimizer.analyze_memory_usage(large_df, "Large DataFrame")

    # Demonstrate caching strategies
    cached_dfs = optimizer.optimize_caching_strategies(large_df)

    # Memory-efficient operations
    sample_df, efficient_df, narrow_ops = optimizer.memory_efficient_operations(large_df)

    # Monitor memory
    executor_info, app_info = optimizer.monitor_memory_metrics()

    # Cleanup
    for df in cached_dfs.values():
        df.unpersist()

    optimizer.spark.stop()
