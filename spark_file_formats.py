"""
Spark File Format Optimization: Parquet, Delta, Iceberg, and Hive
This module demonstrates how to read/write different file formats optimally for ETL performance
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SparkFileFormatHandler:
    def __init__(self, app_name="SparkFileFormats"):
        """Initialize Spark with file format optimizations"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.parquet.enableVectorizedReader", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

    def optimize_parquet_operations(self, df, output_path):
        """Optimize Parquet read/write operations"""
        print("\n=== Parquet Optimization ===")

        # Write optimized Parquet
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .option("parquet.block.size", "134217728") \
            .option("parquet.page.size", "1048576") \
            .parquet(f"{output_path}/parquet_optimized")

        # Read with predicate pushdown
        filtered_df = self.spark.read \
            .option("mergeSchema", "false") \
            .option("pathGlobFilter", "*.parquet") \
            .parquet(f"{output_path}/parquet_optimized") \
            .filter(col("amount") > 100)

        print("Parquet optimized with snappy compression and predicate pushdown")

        # Column pruning example
        selected_cols = self.spark.read \
            .parquet(f"{output_path}/parquet_optimized") \
            .select("id", "amount", "date")  # Only read needed columns

        return filtered_df, selected_cols

    def delta_lake_operations(self, df, delta_path):
        """Demonstrate Delta Lake operations for ACID transactions"""
        print("\n=== Delta Lake Operations ===")

        try:
            # Write to Delta format
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(f"{delta_path}/delta_table")

            # Read from Delta
            delta_df = self.spark.read \
                .format("delta") \
                .load(f"{delta_path}/delta_table")

            # Delta-specific operations
            from delta.tables import DeltaTable

            # Upsert operation (merge)
            delta_table = DeltaTable.forPath(self.spark, f"{delta_path}/delta_table")

            # Time travel queries
            historical_df = self.spark.read \
                .format("delta") \
                .option("timestampAsOf", "2024-01-01") \
                .load(f"{delta_path}/delta_table")

            # Optimize table (compaction)
            delta_table.optimize().executeCompaction()

            # Vacuum old files
            delta_table.vacuum(168)  # Keep 7 days of history

            print("Delta Lake provides ACID transactions, time travel, and schema evolution")

            return delta_df

        except Exception as e:
            print(f"Delta operations require delta-core package: {e}")
            return df

    def iceberg_table_operations(self, df, iceberg_path):
        """Demonstrate Apache Iceberg table operations"""
        print("\n=== Apache Iceberg Operations ===")

        try:
            # Configure Iceberg catalog
            self.spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            self.spark.conf.set("spark.sql.catalog.spark_catalog.type", "hive")

            # Create Iceberg table
            df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .option("path", f"{iceberg_path}/iceberg_table") \
                .saveAsTable("iceberg_demo.transactions")

            # Read from Iceberg
            iceberg_df = self.spark.read \
                .format("iceberg") \
                .load(f"{iceberg_path}/iceberg_table")

            # Iceberg-specific features
            # 1. Schema evolution
            evolved_df = df.withColumn("new_column", lit("default_value"))
            evolved_df.write \
                .format("iceberg") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(f"{iceberg_path}/iceberg_table")

            # 2. Partition evolution
            # 3. Hidden partitioning
            # 4. Time travel

            print("Iceberg provides schema evolution, hidden partitioning, and time travel")

            return iceberg_df

        except Exception as e:
            print(f"Iceberg operations require iceberg-spark package: {e}")
            return df

    def hive_table_operations(self, df, database_name="default", table_name="spark_hive_table"):
        """Demonstrate Hive table operations"""
        print("\n=== Hive Table Operations ===")

        try:
            # Enable Hive support
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            self.spark.sql(f"USE {database_name}")

            # Write to Hive table
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .saveAsTable(f"{database_name}.{table_name}")

            # Read from Hive table
            hive_df = self.spark.table(f"{database_name}.{table_name}")

            # Partitioned Hive table
            partitioned_table = f"{table_name}_partitioned"
            df.write \
                .mode("overwrite") \
                .partitionBy("year", "month") \
                .saveAsTable(f"{database_name}.{partitioned_table}")

            # Dynamic partition pruning
            filtered_hive = self.spark.sql(f"""
                SELECT * FROM {database_name}.{partitioned_table}
                WHERE year = 2024 AND month IN (1, 2, 3)
            """)

            print("Hive tables provide metadata management and partition pruning")

            return hive_df, filtered_hive

        except Exception as e:
            print(f"Hive operations require Hive metastore configuration: {e}")
            return df, df

    def optimize_file_sizes_and_compression(self, df, base_path):
        """Optimize file sizes and compression for different formats"""
        print("\n=== File Size and Compression Optimization ===")

        # Calculate optimal partitions for target file size (128MB-1GB)
        row_count = df.count()
        estimated_size_per_row = 500  # bytes
        target_file_size = 128 * 1024 * 1024  # 128MB
        optimal_partitions = max(1, (row_count * estimated_size_per_row) // target_file_size)

        # Repartition for optimal file sizes
        optimized_df = df.repartition(optimal_partitions)

        # Different compression codecs comparison
        compression_formats = {
            "snappy": {"speed": "fast", "compression": "medium"},
            "gzip": {"speed": "slow", "compression": "high"},
            "lz4": {"speed": "very_fast", "compression": "low"},
            "zstd": {"speed": "medium", "compression": "high"}
        }

        for codec, properties in compression_formats.items():
            try:
                optimized_df.write \
                    .mode("overwrite") \
                    .option("compression", codec) \
                    .parquet(f"{base_path}/compressed_{codec}")

                print(f"{codec}: {properties}")
            except Exception as e:
                print(f"Compression {codec} not available: {e}")

        return optimized_df

    def columnar_vs_row_formats(self, df, base_path):
        """Compare columnar vs row-based storage formats"""
        print("\n=== Columnar vs Row-based Formats ===")

        # Columnar formats (good for analytics)
        # Parquet
        df.write.mode("overwrite").parquet(f"{base_path}/columnar_parquet")

        # ORC
        try:
            df.write.mode("overwrite").format("orc").save(f"{base_path}/columnar_orc")
            print("ORC: Optimized for Hive, good compression")
        except:
            print("ORC format not available")

        # Row-based formats (good for transactional workloads)
        # Avro
        try:
            df.write.mode("overwrite").format("avro").save(f"{base_path}/row_avro")
            print("Avro: Schema evolution, good for streaming")
        except:
            print("Avro format not available")

        # JSON (for flexibility)
        df.write.mode("overwrite").json(f"{base_path}/row_json")

        print("Columnar formats: Better for analytics, column pruning")
        print("Row formats: Better for transactional operations, full row access")

    def streaming_file_formats(self, df, checkpoint_path, output_path):
        """Optimize file formats for streaming operations"""
        print("\n=== Streaming File Format Optimization ===")

        try:
            # Structured streaming with Parquet
            streaming_query = df.writeStream \
                .format("parquet") \
                .option("path", f"{output_path}/streaming_parquet") \
                .option("checkpointLocation", f"{checkpoint_path}/parquet") \
                .trigger(processingTime="10 seconds") \
                .start()

            # Structured streaming with Delta (if available)
            delta_streaming = df.writeStream \
                .format("delta") \
                .option("path", f"{output_path}/streaming_delta") \
                .option("checkpointLocation", f"{checkpoint_path}/delta") \
                .trigger(processingTime="10 seconds") \
                .start()

            print("Streaming optimized for real-time ETL pipelines")

            return streaming_query, delta_streaming

        except Exception as e:
            print(f"Streaming setup error: {e}")
            return None, None

    def read_performance_comparison(self, base_path):
        """Compare read performance across different formats"""
        print("\n=== Read Performance Comparison ===")

        import time

        formats_to_test = [
            ("parquet", f"{base_path}/columnar_parquet"),
            ("json", f"{base_path}/row_json"),
        ]

        for format_name, path in formats_to_test:
            try:
                start_time = time.time()

                if format_name == "parquet":
                    test_df = self.spark.read.parquet(path)
                elif format_name == "json":
                    test_df = self.spark.read.json(path)

                # Trigger action to measure read time
                count = test_df.count()
                read_time = time.time() - start_time

                print(f"{format_name}: {read_time:.2f}s for {count} rows")

            except Exception as e:
                print(f"Could not test {format_name}: {e}")

# Example usage
if __name__ == "__main__":
    handler = SparkFileFormatHandler()

    # Create sample data
    data = [(i, f"customer_{i}", i * 100, 2024, (i % 12) + 1)
            for i in range(100000)]
    df = handler.spark.createDataFrame(data, ["id", "customer", "amount", "year", "month"])

    # Test different formats
    base_path = "/tmp/spark_formats"

    # Parquet optimization
    parquet_df, selected_df = handler.optimize_parquet_operations(df, base_path)

    # File size optimization
    optimized_df = handler.optimize_file_sizes_and_compression(df, base_path)

    # Format comparison
    handler.columnar_vs_row_formats(df, base_path)
    handler.read_performance_comparison(base_path)

    handler.spark.stop()
