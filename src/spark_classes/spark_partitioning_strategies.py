"""
Spark Partitioning Strategies and Repartitioning
This module demonstrates when and how to use partitioning and repartitioning for optimal performance
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SparkPartitioningStrategies:
    def __init__(self, app_name="SparkPartitioning"):
        """Initialize Spark with partitioning optimizations"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .getOrCreate()

    def analyze_partition_distribution(self, df, description="DataFrame"):
        """Analyze current partition distribution"""
        print(f"\n=== {description} Partition Analysis ===")
        print(f"Number of partitions: {df.rdd.getNumPartitions()}")

        # Get records per partition
        partition_sizes = df.rdd.glom().map(len).collect()
        print(f"Records per partition: {partition_sizes}")
        print(f"Total records: {sum(partition_sizes)}")
        print(f"Min partition size: {min(partition_sizes) if partition_sizes else 0}")
        print(f"Max partition size: {max(partition_sizes) if partition_sizes else 0}")
        print(f"Average partition size: {sum(partition_sizes) / len(partition_sizes) if partition_sizes else 0:.2f}")

        return partition_sizes

    def hash_partitioning_by_column(self, df, column_name, num_partitions=None):
        """Partition DataFrame by hash of a specific column"""
        if num_partitions is None:
            num_partitions = self.spark.sparkContext.defaultParallelism

        # Hash partitioning ensures even distribution
        partitioned_df = df.repartition(num_partitions, col(column_name))

        print(f"Hash partitioned by '{column_name}' into {num_partitions} partitions")
        self.analyze_partition_distribution(partitioned_df, f"Hash Partitioned by {column_name}")

        return partitioned_df

    def range_partitioning_by_column(self, df, column_name, num_partitions=None):
        """Partition DataFrame by range of a specific column"""
        if num_partitions is None:
            num_partitions = self.spark.sparkContext.defaultParallelism

        # Range partitioning for ordered data
        partitioned_df = df.repartitionByRange(num_partitions, col(column_name))

        print(f"Range partitioned by '{column_name}' into {num_partitions} partitions")
        self.analyze_partition_distribution(partitioned_df, f"Range Partitioned by {column_name}")

        return partitioned_df

    def when_to_repartition(self, df):
        """Demonstrate scenarios when repartitioning is beneficial"""
        print("\n=== When to Repartition ===")

        # 1. Too many small partitions (each < 128MB)
        current_partitions = df.rdd.getNumPartitions()
        estimated_size_per_partition = df.count() / current_partitions

        if estimated_size_per_partition < 10000:  # Rough threshold
            print("Scenario 1: Too many small partitions - Consider coalescing")
            coalesced_df = df.coalesce(current_partitions // 4)
            self.analyze_partition_distribution(coalesced_df, "Coalesced")

        # 2. Too few large partitions (each > 1GB)
        if estimated_size_per_partition > 1000000:  # Rough threshold
            print("Scenario 2: Too few large partitions - Consider repartitioning")
            repartitioned_df = df.repartition(current_partitions * 4)
            self.analyze_partition_distribution(repartitioned_df, "Repartitioned")

        # 3. Before expensive operations (joins, aggregations)
        print("Scenario 3: Before joins - Partition by join key")
        return df

    def coalesce_vs_repartition(self, df):
        """Demonstrate difference between coalesce and repartition"""
        print("\n=== Coalesce vs Repartition ===")

        original_partitions = df.rdd.getNumPartitions()
        target_partitions = max(1, original_partitions // 2)

        # Coalesce (no shuffle, faster but may cause skew)
        coalesced_df = df.coalesce(target_partitions)
        print(f"Coalesce: {original_partitions} -> {coalesced_df.rdd.getNumPartitions()} partitions")
        self.analyze_partition_distribution(coalesced_df, "Coalesced")

        # Repartition (with shuffle, slower but even distribution)
        repartitioned_df = df.repartition(target_partitions)
        print(f"Repartition: {original_partitions} -> {repartitioned_df.rdd.getNumPartitions()} partitions")
        self.analyze_partition_distribution(repartitioned_df, "Repartitioned")

        return coalesced_df, repartitioned_df

    def partition_by_date_for_time_series(self, df, date_column):
        """Partition time series data by date for efficient querying"""
        # Add year, month, day columns for partitioning
        df_with_date_parts = df \
            .withColumn("year", year(col(date_column))) \
            .withColumn("month", month(col(date_column))) \
            .withColumn("day", dayofmonth(col(date_column)))

        # Partition by year and month for balanced partitions
        partitioned_df = df_with_date_parts.repartition(col("year"), col("month"))

        print("Partitioned by year and month for time series data")
        self.analyze_partition_distribution(partitioned_df, "Date Partitioned")

        return partitioned_df

    def optimize_partitioning_for_joins(self, df1, df2, join_column):
        """Optimize partitioning for efficient joins"""
        print("\n=== Optimizing Partitioning for Joins ===")

        # Partition both DataFrames by join column
        num_partitions = min(200, max(df1.count(), df2.count()) // 10000)

        df1_partitioned = df1.repartition(num_partitions, col(join_column))
        df2_partitioned = df2.repartition(num_partitions, col(join_column))

        print(f"Both DataFrames partitioned by '{join_column}' with {num_partitions} partitions")

        # Perform join
        joined_df = df1_partitioned.join(df2_partitioned, join_column)

        return joined_df

    def partition_for_writing(self, df, output_path, partition_columns=None):
        """Optimize partitioning before writing to storage"""
        if partition_columns:
            # Partition by business logic columns (e.g., date, region)
            df.write \
                .mode("overwrite") \
                .partitionBy(*partition_columns) \
                .parquet(output_path)
            print(f"Data written partitioned by: {partition_columns}")
        else:
            # Optimize number of output files
            num_files = max(1, df.count() // 1000000)  # Target ~1M records per file
            df.repartition(num_files) \
                .write \
                .mode("overwrite") \
                .parquet(output_path)
            print(f"Data written with {num_files} files")

# Example usage
if __name__ == "__main__":
    partitioner = SparkPartitioningStrategies()

    # Create sample data with different scenarios
    data = [(i, f"region_{i%5}", f"2024-{(i%12)+1:02d}-{(i%28)+1:02d}", i * 10)
            for i in range(100000)]
    df = partitioner.spark.createDataFrame(data, ["id", "region", "date", "value"])
    df = df.withColumn("date", to_date(col("date")))

    # Analyze original partitioning
    partitioner.analyze_partition_distribution(df, "Original")

    # Demonstrate different partitioning strategies
    hash_partitioned = partitioner.hash_partitioning_by_column(df, "region", 10)
    range_partitioned = partitioner.range_partitioning_by_column(df, "id", 8)
    date_partitioned = partitioner.partition_by_date_for_time_series(df, "date")

    # Show when to repartition
    partitioner.when_to_repartition(df)

    partitioner.spark.stop()
