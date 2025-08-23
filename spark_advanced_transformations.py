"""
Advanced Spark Data Transformations and ETL Optimization
This module demonstrates advanced transformation techniques for complex ETL scenarios
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

class SparkAdvancedTransformations:
    def __init__(self, app_name="SparkAdvancedETL"):
        """Initialize Spark with advanced transformation optimizations"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

    def complex_data_type_operations(self, df):
        """Handle complex data types: arrays, maps, structs"""
        print("\n=== Complex Data Type Operations ===")

        # Working with arrays
        array_df = df \
            .withColumn("tags", array(lit("tag1"), lit("tag2"), lit("tag3"))) \
            .withColumn("tag_count", size(col("tags"))) \
            .withColumn("first_tag", col("tags")[0]) \
            .withColumn("tags_string", concat_ws(",", col("tags")))

        # Exploding arrays
        exploded_df = array_df \
            .select("id", "name", explode(col("tags")).alias("tag"))

        # Working with maps
        map_df = df \
            .withColumn("attributes",
                       create_map(lit("status"), lit("active"),
                                 lit("priority"), lit("high"))) \
            .withColumn("status", col("attributes")["status"]) \
            .withColumn("map_keys", map_keys(col("attributes"))) \
            .withColumn("map_values", map_values(col("attributes")))

        # Working with structs
        struct_df = df \
            .withColumn("customer_info",
                       struct(col("name").alias("full_name"),
                             col("id").alias("customer_id"))) \
            .withColumn("customer_name", col("customer_info.full_name"))

        print("Complex data types enable nested data processing")
        return array_df, exploded_df, map_df, struct_df

    def advanced_string_transformations(self, df):
        """Advanced string processing and text transformations"""
        print("\n=== Advanced String Transformations ===")

        # String cleaning and normalization
        cleaned_df = df \
            .withColumn("name_cleaned",
                       trim(lower(regexp_replace(col("name"), "[^a-zA-Z0-9\\s]", "")))) \
            .withColumn("name_standardized",
                       regexp_replace(col("name_cleaned"), "\\s+", " "))

        # Text extraction with regex
        extracted_df = cleaned_df \
            .withColumn("name_parts", split(col("name_standardized"), " ")) \
            .withColumn("first_name", col("name_parts")[0]) \
            .withColumn("last_name", col("name_parts")[size(col("name_parts")) - 1])

        # Advanced string functions
        string_ops_df = extracted_df \
            .withColumn("name_length", length(col("name"))) \
            .withColumn("name_hash", hash(col("name"))) \
            .withColumn("name_base64", base64(col("name").cast("binary"))) \
            .withColumn("name_soundex", soundex(col("name")))

        # JSON processing
        json_df = df \
            .withColumn("json_data",
                       to_json(struct(col("id"), col("name"), col("amount")))) \
            .withColumn("parsed_json", from_json(col("json_data"),
                       StructType([
                           StructField("id", IntegerType()),
                           StructField("name", StringType()),
                           StructField("amount", DoubleType())
                       ])))

        return cleaned_df, extracted_df, string_ops_df, json_df

    def advanced_date_time_operations(self, df):
        """Advanced date and time transformations"""
        print("\n=== Advanced Date/Time Operations ===")

        # Current date/time operations
        datetime_df = df \
            .withColumn("current_timestamp", current_timestamp()) \
            .withColumn("current_date", current_date()) \
            .withColumn("processing_date", date_format(current_date(), "yyyy-MM-dd"))

        # Date arithmetic and formatting
        date_ops_df = datetime_df \
            .withColumn("date_plus_30", date_add(col("current_date"), 30)) \
            .withColumn("date_minus_7", date_sub(col("current_date"), 7)) \
            .withColumn("months_between", months_between(col("current_date"), col("date_plus_30"))) \
            .withColumn("quarter", quarter(col("current_date"))) \
            .withColumn("week_of_year", weekofyear(col("current_date")))

        # Time zone operations
        timezone_df = date_ops_df \
            .withColumn("utc_timestamp", to_utc_timestamp(col("current_timestamp"), "PST")) \
            .withColumn("local_timestamp", from_utc_timestamp(col("utc_timestamp"), "EST"))

        # Business date calculations
        business_df = timezone_df \
            .withColumn("is_weekend", dayofweek(col("current_date")).isin([1, 7])) \
            .withColumn("business_day",
                       when(col("is_weekend"), date_add(col("current_date"), 2))
                       .otherwise(col("current_date")))

        return datetime_df, date_ops_df, timezone_df, business_df

    def advanced_window_operations(self, df):
        """Advanced window function operations for analytics"""
        print("\n=== Advanced Window Operations ===")

        # Multiple window specifications
        partition_window = Window.partitionBy("category")
        ordered_window = Window.partitionBy("category").orderBy("date")
        range_window = Window.partitionBy("category").orderBy("date") \
                             .rangeBetween(-86400, 0)  # 1 day in seconds

        # Ranking and analytical functions
        ranking_df = df \
            .withColumn("row_number", row_number().over(ordered_window)) \
            .withColumn("rank", rank().over(ordered_window)) \
            .withColumn("dense_rank", dense_rank().over(ordered_window)) \
            .withColumn("percent_rank", percent_rank().over(ordered_window)) \
            .withColumn("ntile", ntile(4).over(ordered_window))

        # Lag/Lead operations
        lag_lead_df = ranking_df \
            .withColumn("prev_amount", lag("amount", 1).over(ordered_window)) \
            .withColumn("next_amount", lead("amount", 1).over(ordered_window)) \
            .withColumn("amount_change", col("amount") - col("prev_amount")) \
            .withColumn("amount_change_pct",
                       (col("amount") - col("prev_amount")) / col("prev_amount") * 100)

        # Moving aggregations
        moving_agg_df = lag_lead_df \
            .withColumn("moving_avg_3", avg("amount").over(
                ordered_window.rowsBetween(-2, 0))) \
            .withColumn("moving_sum_7", sum("amount").over(
                ordered_window.rowsBetween(-6, 0))) \
            .withColumn("running_total", sum("amount").over(
                ordered_window.rowsBetween(Window.unboundedPreceding, 0)))

        # First/Last value operations
        first_last_df = moving_agg_df \
            .withColumn("first_amount", first("amount").over(ordered_window)) \
            .withColumn("last_amount", last("amount").over(ordered_window)) \
            .withColumn("min_amount", min("amount").over(partition_window)) \
            .withColumn("max_amount", max("amount").over(partition_window))

        return ranking_df, lag_lead_df, moving_agg_df, first_last_df

    def data_quality_transformations(self, df):
        """Data quality checks and cleansing transformations"""
        print("\n=== Data Quality Transformations ===")

        # Null handling strategies
        null_handled_df = df \
            .withColumn("name_cleaned", coalesce(col("name"), lit("UNKNOWN"))) \
            .withColumn("amount_cleaned",
                       when(col("amount").isNull() | (col("amount") < 0), 0)
                       .otherwise(col("amount"))) \
            .fillna({"category": "OTHER", "status": "PENDING"})

        # Duplicate detection
        duplicate_check_df = null_handled_df \
            .withColumn("row_hash", hash(col("name"), col("category"))) \
            .withColumn("duplicate_count",
                       count("*").over(Window.partitionBy("row_hash"))) \
            .withColumn("is_duplicate", col("duplicate_count") > 1)

        # Outlier detection using IQR method
        stats_df = duplicate_check_df \
            .select(
                expr("percentile_approx(amount, 0.25)").alias("q1"),
                expr("percentile_approx(amount, 0.75)").alias("q3")
            ).collect()[0]

        iqr = stats_df["q3"] - stats_df["q1"]
        lower_bound = stats_df["q1"] - 1.5 * iqr
        upper_bound = stats_df["q3"] + 1.5 * iqr

        outlier_df = duplicate_check_df \
            .withColumn("is_outlier",
                       (col("amount") < lower_bound) | (col("amount") > upper_bound)) \
            .withColumn("amount_capped",
                       when(col("amount") > upper_bound, upper_bound)
                       .when(col("amount") < lower_bound, lower_bound)
                       .otherwise(col("amount")))

        # Data validation rules
        validated_df = outlier_df \
            .withColumn("validation_errors", array()) \
            .withColumn("validation_errors",
                       when(col("name").isNull() | (col("name") == ""),
                            array_union(col("validation_errors"), array(lit("MISSING_NAME"))))
                       .otherwise(col("validation_errors"))) \
            .withColumn("validation_errors",
                       when(col("amount") < 0,
                            array_union(col("validation_errors"), array(lit("NEGATIVE_AMOUNT"))))
                       .otherwise(col("validation_errors"))) \
            .withColumn("is_valid", size(col("validation_errors")) == 0)

        return null_handled_df, duplicate_check_df, outlier_df, validated_df

    def performance_optimized_transformations(self, df):
        """Performance-optimized transformation patterns"""
        print("\n=== Performance-Optimized Transformations ===")

        # Efficient filtering and selection
        optimized_df = df \
            .filter(col("amount") > 0) \
            .filter(col("status").isin(["active", "pending"])) \
            .select("id", "name", "amount", "category")

        # Efficient joins with broadcast
        small_lookup = df.select("category").distinct() \
                        .withColumn("category_type",
                                   when(col("category").like("%premium%"), "premium")
                                   .otherwise("standard"))

        joined_df = optimized_df.join(broadcast(small_lookup), "category")

        # Columnar operations (vectorized)
        vectorized_df = joined_df \
            .withColumn("amount_squared", col("amount") * col("amount")) \
            .withColumn("log_amount", log(col("amount") + 1)) \
            .withColumn("normalized_amount",
                       (col("amount") - avg("amount").over(Window.partitionBy("category"))) /
                       stddev("amount").over(Window.partitionBy("category")))

        # Batch processing for large transformations
        # Cache intermediate results
        vectorized_df.cache()

        # Multiple transformations on cached data
        result1 = vectorized_df.groupBy("category").sum("amount")
        result2 = vectorized_df.filter(col("amount_squared") > 1000000)

        return optimized_df, joined_df, vectorized_df, result1, result2

# Example usage
if __name__ == "__main__":
    transformer = SparkAdvancedTransformations()

    # Create sample data with various data quality issues
    import random
    from datetime import datetime, timedelta

    data = []
    for i in range(10000):
        name = f"Customer_{i}" if random.random() > 0.05 else None  # 5% null names
        amount = random.randint(-100, 10000) if random.random() > 0.03 else None  # 3% null amounts
        category = random.choice(["premium", "standard", "basic", None])
        status = random.choice(["active", "pending", "inactive"])
        date = datetime.now() - timedelta(days=random.randint(0, 365))

        data.append((i, name, amount, category, status, date))

    df = transformer.spark.createDataFrame(data,
        ["id", "name", "amount", "category", "status", "date"])

    # Demonstrate transformations
    array_df, exploded_df, map_df, struct_df = transformer.complex_data_type_operations(df)

    # Data quality transformations
    null_handled, duplicate_check, outlier_df, validated_df = transformer.data_quality_transformations(df)

    print("Validation errors summary:")
    validated_df.groupBy("is_valid").count().show()

    # Performance optimizations
    optimized, joined, vectorized, result1, result2 = transformer.performance_optimized_transformations(df)

    transformer.spark.stop()
