"""
Spark Advanced Data Writing Strategies
This module demonstrates optimal writing patterns for bucketed tables, partitioned data,
and different storage formats with data engineering best practices
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SparkAdvancedWritingStrategies:
    def __init__(self, app_name="SparkAdvancedWriter"):
        """Initialize Spark with advanced writing optimizations"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.sources.bucketing.enabled", "true") \
            .config("spark.sql.sources.bucketing.autoBucketedScan.enabled", "true") \
            .config("spark.sql.hive.metastorePartitionPruning", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

    def write_bucketed_table(self, df, output_path, table_name, bucket_columns,
                           num_buckets, sort_columns=None, file_format="parquet"):
        """Write data as bucketed table for optimal join performance"""
        print(f"\n=== Writing Bucketed Table: {table_name} ===")
        print(f"Bucket columns: {bucket_columns}")
        print(f"Number of buckets: {num_buckets}")
        print(f"Sort columns: {sort_columns}")

        # Create the bucketed table write operation
        writer = df.write \
            .mode("overwrite") \
            .option("path", output_path) \
            .bucketBy(num_buckets, *bucket_columns)

        # Add sort columns if specified
        if sort_columns:
            writer = writer.sortBy(*sort_columns)

        # Write based on format
        if file_format.lower() == "parquet":
            writer.option("compression", "snappy").saveAsTable(table_name)
        elif file_format.lower() == "orc":
            writer.format("orc").option("compression", "zlib").saveAsTable(table_name)
        elif file_format.lower() == "delta":
            writer.format("delta").saveAsTable(table_name)
        else:
            writer.saveAsTable(table_name)

        print(f"Bucketed table saved as {table_name} in {file_format} format")

        # Analyze bucketing effectiveness
        self._analyze_bucketing_distribution(table_name, bucket_columns, num_buckets)

        return table_name

    def write_partitioned_table(self, df, output_path, table_name, partition_columns,
                              file_format="parquet", dynamic_partitioning=True):
        """Write data as partitioned table with optimization"""
        print(f"\n=== Writing Partitioned Table: {table_name} ===")
        print(f"Partition columns: {partition_columns}")
        print(f"Dynamic partitioning: {dynamic_partitioning}")

        # Configure dynamic partitioning
        if dynamic_partitioning:
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            self.spark.conf.set("hive.exec.dynamic.partition", "true")
            self.spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
            self.spark.conf.set("hive.exec.max.dynamic.partitions", "10000")

        # Optimize partitioning before write
        optimized_df = self._optimize_for_partitioned_write(df, partition_columns)

        # Write partitioned table
        writer = optimized_df.write \
            .mode("overwrite") \
            .option("path", output_path) \
            .partitionBy(*partition_columns)

        if file_format.lower() == "parquet":
            writer.option("compression", "snappy") \
                  .option("parquet.block.size", "134217728") \
                  .saveAsTable(table_name)
        elif file_format.lower() == "orc":
            writer.format("orc") \
                  .option("compression", "zlib") \
                  .option("orc.stripe.size", "67108864") \
                  .saveAsTable(table_name)
        elif file_format.lower() == "delta":
            writer.format("delta") \
                  .option("mergeSchema", "true") \
                  .saveAsTable(table_name)
        else:
            writer.saveAsTable(table_name)

        print(f"Partitioned table saved as {table_name}")

        # Analyze partitioning effectiveness
        self._analyze_partitioning_distribution(table_name, partition_columns)

        return table_name

    def write_bucketed_and_partitioned_table(self, df, output_path, table_name,
                                           partition_columns, bucket_columns, num_buckets,
                                           sort_columns=None, file_format="parquet"):
        """Write data with both partitioning and bucketing for maximum optimization"""
        print(f"\n=== Writing Bucketed + Partitioned Table: {table_name} ===")
        print(f"Partition columns: {partition_columns}")
        print(f"Bucket columns: {bucket_columns}")
        print(f"Number of buckets: {num_buckets}")

        # Enable dynamic partitioning
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # Optimize DataFrame for both partitioning and bucketing
        optimized_df = self._optimize_for_bucketed_partitioned_write(
            df, partition_columns, bucket_columns, num_buckets
        )

        # Create writer with both partitioning and bucketing
        writer = optimized_df.write \
            .mode("overwrite") \
            .option("path", output_path) \
            .partitionBy(*partition_columns) \
            .bucketBy(num_buckets, *bucket_columns)

        if sort_columns:
            writer = writer.sortBy(*sort_columns)

        # Format-specific optimizations
        if file_format.lower() == "parquet":
            writer.option("compression", "snappy") \
                  .option("parquet.block.size", "134217728") \
                  .saveAsTable(table_name)
        elif file_format.lower() == "orc":
            writer.format("orc") \
                  .option("compression", "zlib") \
                  .saveAsTable(table_name)

        print(f"Bucketed + Partitioned table saved as {table_name}")
        return table_name

    def write_for_initial_load_optimization(self, df, output_path, file_format="parquet",
                                          target_file_size_mb=128, compression="snappy"):
        """Optimize writing for initial data loads (non-bucketed/partitioned)"""
        print(f"\n=== Initial Load Optimization ===")
        print("Writing to non-bucketed, non-partitioned format for initial load speed")

        # Calculate optimal number of files
        estimated_size_mb = self._estimate_dataframe_size_mb(df)
        num_files = max(1, int(estimated_size_mb / target_file_size_mb))

        print(f"Estimated DataFrame size: {estimated_size_mb:.2f} MB")
        print(f"Target file size: {target_file_size_mb} MB")
        print(f"Writing {num_files} files")

        # Repartition for optimal file sizes
        optimized_df = df.repartition(num_files)

        if file_format.lower() == "parquet":
            optimized_df.write \
                .mode("overwrite") \
                .option("compression", compression) \
                .option("parquet.block.size", str(target_file_size_mb * 1024 * 1024)) \
                .parquet(output_path)
        elif file_format.lower() == "orc":
            optimized_df.write \
                .mode("overwrite") \
                .format("orc") \
                .option("compression", compression) \
                .save(output_path)
        elif file_format.lower() == "delta":
            optimized_df.write \
                .mode("overwrite") \
                .format("delta") \
                .option("compression", compression) \
                .save(output_path)
        elif file_format.lower() == "json":
            optimized_df.write \
                .mode("overwrite") \
                .option("compression", compression) \
                .json(output_path)
        elif file_format.lower() == "csv":
            optimized_df.write \
                .mode("overwrite") \
                .option("compression", compression) \
                .option("header", "true") \
                .csv(output_path)

        print(f"Initial load completed to {output_path}")
        return output_path

    def write_with_z_ordering(self, df, output_path, z_order_columns, file_format="delta"):
        """Write data with Z-ordering for improved query performance (Delta Lake)"""
        if file_format.lower() != "delta":
            print("Z-ordering is only supported with Delta Lake format")
            return None

        print(f"\n=== Writing with Z-Ordering ===")
        print(f"Z-order columns: {z_order_columns}")

        # Write Delta table
        df.write \
            .mode("overwrite") \
            .format("delta") \
            .save(output_path)

        # Apply Z-ordering
        try:
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(self.spark, output_path)
            delta_table.optimize().executeZOrderBy(*z_order_columns)
            print(f"Z-ordering applied on columns: {z_order_columns}")
        except ImportError:
            print("Delta Lake not available for Z-ordering")

        return output_path

    def write_streaming_optimized(self, streaming_df, output_path, checkpoint_path,
                                file_format="delta", trigger_interval="10 seconds",
                                partition_columns=None):
        """Write streaming data with optimizations"""
        print(f"\n=== Streaming Write Optimization ===")

        writer = streaming_df.writeStream \
            .format(file_format) \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime=trigger_interval) \
            .outputMode("append")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
            print(f"Streaming with partitioning: {partition_columns}")

        if file_format.lower() == "delta":
            writer = writer.option("mergeSchema", "true")
        elif file_format.lower() == "parquet":
            writer = writer.option("compression", "snappy")

        query = writer.start()
        print(f"Streaming query started with {trigger_interval} trigger")

        return query

    def _optimize_for_partitioned_write(self, df, partition_columns):
        """Optimize DataFrame for partitioned writing"""
        print("Optimizing for partitioned write...")

        # Check partition distribution
        partition_stats = df.groupBy(*partition_columns).count().collect()
        print(f"Writing {len(partition_stats)} partitions")

        # Optimize partition sizes
        total_rows = df.count()
        if total_rows > 10000000:  # Large dataset
            # Use hash partitioning to distribute evenly
            return df.repartition(*[col(c) for c in partition_columns])
        else:
            # Use coalesce to avoid small files
            return df.coalesce(max(1, len(partition_stats) // 4))

    def _optimize_for_bucketed_partitioned_write(self, df, partition_columns,
                                               bucket_columns, num_buckets):
        """Optimize DataFrame for both bucketing and partitioning"""
        print("Optimizing for bucketed + partitioned write...")

        # First partition by partition columns, then by bucket columns
        partition_cols = [col(c) for c in partition_columns]
        bucket_cols = [col(c) for c in bucket_columns]

        return df.repartition(num_buckets, *(partition_cols + bucket_cols))

    def _estimate_dataframe_size_mb(self, df):
        """Estimate DataFrame size in MB"""
        # Rough estimation based on row count and column count
        row_count = df.count()
        col_count = len(df.columns)

        # Assume average 10 bytes per field (very rough)
        estimated_bytes = row_count * col_count * 10
        estimated_mb = estimated_bytes / (1024 * 1024)

        return estimated_mb

    def _analyze_bucketing_distribution(self, table_name, bucket_columns, num_buckets):
        """Analyze bucketing distribution effectiveness"""
        print(f"\n--- Bucketing Analysis for {table_name} ---")

        try:
            # Create a hash column to simulate bucketing
            table_df = self.spark.table(table_name)

            # Show distribution across buckets
            bucket_analysis = table_df \
                .withColumn("bucket_id",
                           abs(hash(*[col(c) for c in bucket_columns])) % num_buckets) \
                .groupBy("bucket_id") \
                .count() \
                .orderBy("bucket_id")

            print("Bucket distribution:")
            bucket_analysis.show()

            # Calculate skew
            bucket_counts = [row["count"] for row in bucket_analysis.collect()]
            if bucket_counts:
                avg_count = sum(bucket_counts) / len(bucket_counts)
                max_count = max(bucket_counts)
                min_count = min(bucket_counts)
                skew_ratio = max_count / avg_count if avg_count > 0 else 0

                print(f"Bucketing effectiveness:")
                print(f"  Average rows per bucket: {avg_count:.0f}")
                print(f"  Min rows per bucket: {min_count}")
                print(f"  Max rows per bucket: {max_count}")
                print(f"  Skew ratio: {skew_ratio:.2f}")

                if skew_ratio > 2:
                    print("  WARNING: High skew detected in bucketing!")
                elif skew_ratio > 1.5:
                    print("  CAUTION: Moderate skew in bucketing")
                else:
                    print("  GOOD: Well-distributed bucketing")

        except Exception as e:
            print(f"Could not analyze bucketing: {e}")

    def _analyze_partitioning_distribution(self, table_name, partition_columns):
        """Analyze partitioning distribution effectiveness"""
        print(f"\n--- Partitioning Analysis for {table_name} ---")

        try:
            table_df = self.spark.table(table_name)

            # Show partition distribution
            partition_analysis = table_df \
                .groupBy(*partition_columns) \
                .count() \
                .orderBy(desc("count"))

            print("Top 10 partitions by size:")
            partition_analysis.show(10)

            # Calculate partition statistics
            partition_counts = [row["count"] for row in partition_analysis.collect()]
            if partition_counts:
                total_partitions = len(partition_counts)
                avg_size = sum(partition_counts) / total_partitions
                max_size = max(partition_counts)
                min_size = min(partition_counts)

                print(f"Partitioning effectiveness:")
                print(f"  Total partitions: {total_partitions}")
                print(f"  Average partition size: {avg_size:.0f} rows")
                print(f"  Largest partition: {max_size} rows")
                print(f"  Smallest partition: {min_size} rows")

                # Check for small partitions
                small_partitions = sum(1 for count in partition_counts if count < 1000)
                if small_partitions > total_partitions * 0.3:
                    print(f"  WARNING: {small_partitions} small partitions detected!")
                    print("  Consider fewer partition columns or different strategy")

        except Exception as e:
            print(f"Could not analyze partitioning: {e}")

    def optimize_file_formats(self, df, output_path, formats=None):
        """Optimize and compare different file formats for the given DataFrame"""
        print(f"\n=== File Format Optimization ===")
        print(f"Testing different file formats for optimal performance")

        if formats is None:
            formats = ["parquet", "orc", "delta", "json", "csv"]

        results = {}

        for file_format in formats:
            try:
                format_path = f"{output_path}_{file_format}"
                print(f"\nTesting {file_format.upper()} format...")

                # Measure write performance
                import time
                start_time = time.time()

                if file_format.lower() == "parquet":
                    df.write.mode("overwrite") \
                        .option("compression", "snappy") \
                        .parquet(format_path)
                elif file_format.lower() == "orc":
                    df.write.mode("overwrite") \
                        .format("orc") \
                        .option("compression", "zlib") \
                        .save(format_path)
                elif file_format.lower() == "delta":
                    df.write.mode("overwrite") \
                        .format("delta") \
                        .save(format_path)
                elif file_format.lower() == "json":
                    df.write.mode("overwrite") \
                        .option("compression", "gzip") \
                        .json(format_path)
                elif file_format.lower() == "csv":
                    df.write.mode("overwrite") \
                        .option("compression", "gzip") \
                        .option("header", "true") \
                        .csv(format_path)

                write_time = time.time() - start_time

                # Test read performance
                start_time = time.time()
                if file_format.lower() == "parquet":
                    test_df = self.spark.read.parquet(format_path)
                elif file_format.lower() == "orc":
                    test_df = self.spark.read.format("orc").load(format_path)
                elif file_format.lower() == "delta":
                    test_df = self.spark.read.format("delta").load(format_path)
                elif file_format.lower() == "json":
                    test_df = self.spark.read.json(format_path)
                elif file_format.lower() == "csv":
                    test_df = self.spark.read.option("header", "true").csv(format_path)

                # Force action to measure read time
                row_count = test_df.count()
                read_time = time.time() - start_time

                results[file_format] = {
                    "write_time": write_time,
                    "read_time": read_time,
                    "total_time": write_time + read_time,
                    "row_count": row_count,
                    "path": format_path
                }

                print(f"  Write time: {write_time:.2f}s")
                print(f"  Read time: {read_time:.2f}s")
                print(f"  Total time: {write_time + read_time:.2f}s")

            except Exception as e:
                print(f"  ERROR with {file_format}: {e}")
                results[file_format] = {"error": str(e)}

        # Find best performing format
        valid_results = {k: v for k, v in results.items() if "error" not in v}
        if valid_results:
            best_format = min(valid_results.keys(),
                            key=lambda x: valid_results[x]["total_time"])
            print(f"\n🏆 Best performing format: {best_format.upper()}")
            print(f"   Total time: {valid_results[best_format]['total_time']:.2f}s")

        return results

    def demonstrate_writing_strategies(self, df):
        """Demonstrate different writing strategies for various scenarios"""
        print("=== Writing Strategy Demonstrations ===")

        base_path = "/tmp/spark_writing_demo"

        # 1. Initial load - fast writing, no bucketing/partitioning
        print("\n1. INITIAL LOAD STRATEGY")
        print("Best for: First-time data loads, staging tables")
        initial_load_path = f"{base_path}/initial_load"
        self.write_for_initial_load_optimization(df, initial_load_path)

        # 2. Partitioned table - for query optimization
        print("\n2. PARTITIONED TABLE STRATEGY")
        print("Best for: Time-series data, frequently filtered columns")
        partitioned_table = "demo_partitioned_table"
        self.write_partitioned_table(df, f"{base_path}/partitioned",
                                   partitioned_table, ["year", "month"])

        # 3. Bucketed table - for join optimization
        print("\n3. BUCKETED TABLE STRATEGY")
        print("Best for: Frequently joined tables, dimension tables")
        bucketed_table = "demo_bucketed_table"
        self.write_bucketed_table(df, f"{base_path}/bucketed",
                                bucketed_table, ["customer_id"],
                                num_buckets=10, sort_columns=["customer_id"])

        # 4. Combined strategy - maximum optimization
        print("\n4. COMBINED STRATEGY")
        print("Best for: Large fact tables with complex query patterns")
        combined_table = "demo_combined_table"
        self.write_bucketed_and_partitioned_table(df, f"{base_path}/combined",
                                                 combined_table,
                                                 partition_columns=["year"],
                                                 bucket_columns=["customer_id"],
                                                 num_buckets=8)

# Example usage and best practices
if __name__ == "__main__":
    writer = SparkAdvancedWritingStrategies()

    # Create sample data
    data = [
        (i, f"customer_{i%1000}", f"product_{i%100}", i * 10,
         2024, (i%12)+1, (i%28)+1, f"region_{i%4}")
        for i in range(100000)
    ]

    df = writer.spark.createDataFrame(data,
        ["id", "customer_id", "product_id", "amount", "year", "month", "day", "region"])

    # Demonstrate all writing strategies
    writer.demonstrate_writing_strategies(df)

    writer.spark.stop()
