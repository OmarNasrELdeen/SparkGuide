"""
Spark Grouping Strategies and Aggregation Optimization
This module demonstrates when and how to use different grouping strategies for optimal performance
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

class SparkGroupingStrategies:
    def __init__(self, app_name="SparkGrouping"):
        """Initialize Spark with grouping optimizations"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

    def when_to_group_vs_window(self, df):
        """Demonstrate when to use groupBy vs window functions"""
        print("\n=== GroupBy vs Window Functions ===")

        # Use groupBy when you need aggregated results only
        grouped_result = df \
            .groupBy("category", "region") \
            .agg(
                sum("sales").alias("total_sales"),
                count("*").alias("transaction_count"),
                avg("sales").alias("avg_sales")
            )

        print("Use groupBy for: Final aggregated results, reducing data size")

        # Use window functions when you need both detail and aggregated data
        window_spec = Window.partitionBy("category", "region")
        windowed_result = df \
            .withColumn("total_sales", sum("sales").over(window_spec)) \
            .withColumn("avg_sales", avg("sales").over(window_spec)) \
            .withColumn("sales_rank", dense_rank().over(
                Window.partitionBy("category", "region").orderBy(desc("sales"))
            ))

        print("Use window functions for: Ranking, running totals, keeping detail rows")

        return grouped_result, windowed_result

    def optimize_multi_level_grouping(self, df):
        """Optimize multi-level grouping operations"""
        print("\n=== Multi-Level Grouping Optimization ===")

        # Instead of multiple separate groupings, use rollup/cube
        # Traditional approach (less efficient)
        region_totals = df.groupBy("region").agg(sum("sales").alias("sales"))
        category_totals = df.groupBy("category").agg(sum("sales").alias("sales"))
        overall_total = df.agg(sum("sales").alias("sales"))

        # Optimized approach using rollup
        rollup_result = df \
            .rollup("region", "category") \
            .agg(sum("sales").alias("total_sales"),
                 count("*").alias("count")) \
            .orderBy("region", "category")

        print("Rollup provides hierarchical aggregations in single operation")

        # Use cube for all combinations
        cube_result = df \
            .cube("region", "category", "product_type") \
            .agg(sum("sales").alias("total_sales")) \
            .filter(col("total_sales").isNotNull())

        print("Cube provides all possible combinations of grouping columns")

        return rollup_result, cube_result

    def handle_skewed_grouping(self, df, skewed_column):
        """Handle data skew in grouping operations"""
        print(f"\n=== Handling Skew in '{skewed_column}' ===")

        # Analyze skew
        skew_analysis = df \
            .groupBy(skewed_column) \
            .count() \
            .orderBy(desc("count"))

        print("Top skewed values:")
        skew_analysis.show(10)

        # Strategy 1: Salt the skewed keys
        salted_df = df \
            .withColumn("salt", (rand() * 10).cast("int")) \
            .withColumn("salted_key", concat(col(skewed_column), lit("_"), col("salt")))

        # Group by salted key first
        salted_grouped = salted_df \
            .groupBy("salted_key") \
            .agg(sum("sales").alias("partial_sum"))

        # Then aggregate the partial results
        final_result = salted_grouped \
            .withColumn("original_key", regexp_replace(col("salted_key"), "_\\d+$", "")) \
            .groupBy("original_key") \
            .agg(sum("partial_sum").alias("total_sales"))

        print("Salting technique applied to reduce skew")

        return final_result

    def optimize_grouping_with_filters(self, df):
        """Optimize grouping operations with filters"""
        print("\n=== Optimizing Grouping with Filters ===")

        # Apply filters before grouping to reduce data size
        filtered_grouped = df \
            .filter(col("status") == "completed") \
            .filter(col("amount") > 0) \
            .groupBy("customer_segment", "product_category") \
            .agg(
                sum("amount").alias("total_amount"),
                countDistinct("customer_id").alias("unique_customers"),
                avg("amount").alias("avg_amount")
            )

        # Use having clause for post-aggregation filtering
        having_filtered = filtered_grouped \
            .filter(col("total_amount") > 10000) \
            .filter(col("unique_customers") > 5)

        return having_filtered

    def advanced_aggregation_functions(self, df):
        """Demonstrate advanced aggregation functions"""
        print("\n=== Advanced Aggregation Functions ===")

        # Collect list and set aggregations
        list_agg = df \
            .groupBy("category") \
            .agg(
                collect_list("product_name").alias("all_products"),
                collect_set("product_name").alias("unique_products"),
                sort_array(collect_set("product_name")).alias("sorted_products")
            )

        # Statistical aggregations
        stats_agg = df \
            .groupBy("region") \
            .agg(
                stddev("sales").alias("sales_stddev"),
                variance("sales").alias("sales_variance"),
                skewness("sales").alias("sales_skewness"),
                kurtosis("sales").alias("sales_kurtosis")
            )

        # Percentile aggregations
        percentile_agg = df \
            .groupBy("category") \
            .agg(
                expr("percentile_approx(sales, 0.25)").alias("q1"),
                expr("percentile_approx(sales, 0.5)").alias("median"),
                expr("percentile_approx(sales, 0.75)").alias("q3"),
                expr("percentile_approx(sales, 0.95)").alias("p95")
            )

        return list_agg, stats_agg, percentile_agg

    def optimize_grouping_for_time_series(self, df, date_column):
        """Optimize grouping for time series data"""
        print("\n=== Time Series Grouping Optimization ===")

        # Add time-based grouping columns
        time_grouped_df = df \
            .withColumn("year", year(col(date_column))) \
            .withColumn("month", month(col(date_column))) \
            .withColumn("week", weekofyear(col(date_column))) \
            .withColumn("day_of_week", dayofweek(col(date_column)))

        # Daily aggregations
        daily_agg = time_grouped_df \
            .groupBy("year", "month", dayofmonth(col(date_column)).alias("day")) \
            .agg(sum("sales").alias("daily_sales"))

        # Weekly aggregations
        weekly_agg = time_grouped_df \
            .groupBy("year", "week") \
            .agg(sum("sales").alias("weekly_sales"))

        # Monthly aggregations with moving averages
        monthly_agg = time_grouped_df \
            .groupBy("year", "month") \
            .agg(sum("sales").alias("monthly_sales")) \
            .withColumn("3_month_avg",
                       avg("monthly_sales").over(
                           Window.orderBy("year", "month")
                           .rowsBetween(-2, 0)
                       ))

        return daily_agg, weekly_agg, monthly_agg

    def memory_efficient_grouping(self, df):
        """Implement memory-efficient grouping strategies"""
        print("\n=== Memory-Efficient Grouping ===")

        # Use approximate functions for large datasets
        approx_agg = df \
            .groupBy("category") \
            .agg(
                approx_count_distinct("customer_id").alias("approx_unique_customers"),
                expr("percentile_approx(sales, 0.5)").alias("approx_median")
            )

        # Incremental aggregation for very large datasets
        # Process data in chunks if memory is limited
        sample_fraction = 0.1
        sample_agg = df \
            .sample(sample_fraction) \
            .groupBy("category") \
            .agg((sum("sales") / sample_fraction).alias("estimated_total_sales"))

        print("Approximate aggregations reduce memory usage")

        return approx_agg, sample_agg

    def partition_aware_grouping(self, df, partition_column):
        """Optimize grouping considering data partitioning"""
        print(f"\n=== Partition-Aware Grouping by '{partition_column}' ===")

        # When grouping by partition column, no shuffle needed
        partition_grouped = df \
            .groupBy(partition_column) \
            .agg(sum("sales").alias("total_sales"))

        print("Grouping by partition column avoids shuffle")

        # When grouping by non-partition column, consider repartitioning first
        # if the group will be used multiple times
        non_partition_grouped = df \
            .repartition(col("category")) \
            .groupBy("category") \
            .agg(sum("sales").alias("category_sales"))

        return partition_grouped, non_partition_grouped

# Example usage
if __name__ == "__main__":
    grouper = SparkGroupingStrategies()

    # Create sample time series data
    from datetime import datetime, timedelta
    import random

    base_date = datetime(2024, 1, 1)
    data = []
    for i in range(100000):
        date = base_date + timedelta(days=random.randint(0, 365))
        data.append((
            i,
            f"category_{i % 5}",
            f"region_{i % 3}",
            f"product_{i % 20}",
            random.randint(100, 1000),
            date.strftime("%Y-%m-%d"),
            "completed" if random.random() > 0.1 else "pending"
        ))

    df = grouper.spark.createDataFrame(data,
        ["id", "category", "region", "product_name", "sales", "date", "status"])
    df = df.withColumn("date", to_date(col("date")))

    # Demonstrate grouping strategies
    grouped, windowed = grouper.when_to_group_vs_window(df)
    grouped.show(10)

    # Handle skewed data
    skewed_result = grouper.handle_skewed_grouping(df, "category")
    skewed_result.show()

    # Time series grouping
    daily, weekly, monthly = grouper.optimize_grouping_for_time_series(df, "date")
    monthly.show()

    grouper.spark.stop()
