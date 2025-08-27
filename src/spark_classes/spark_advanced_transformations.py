"""
Advanced Spark Data Transformations and ETL Optimization
This module demonstrates advanced transformation techniques for complex ETL scenarios
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
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

        # Use product_name as the base string column when present
        name_col = "product_name" if "product_name" in df.columns else ("name" if "name" in df.columns else None)

        # Working with arrays
        array_df = df \
            .withColumn("tags", array(lit("tag1"), lit("tag2"), lit("tag3"))) \
            .withColumn("tag_count", size(col("tags"))) \
            .withColumn("first_tag", col("tags")[0]) \
            .withColumn("tags_string", concat_ws(",", col("tags")))

        # Exploding arrays
        if name_col:
            exploded_df = array_df.select("id", col(name_col).alias("name"), explode(col("tags")).alias("tag"))
        else:
            exploded_df = array_df.select("id", explode(col("tags")).alias("tag"))

        # Working with maps
        map_df = df \
            .withColumn("attributes",
                       create_map(lit("status"), coalesce(col("status"), lit("active")),
                                  lit("priority"), lit("high"))) \
            .withColumn("status", col("attributes")["status"]) \
            .withColumn("map_keys", map_keys(col("attributes"))) \
            .withColumn("map_values", map_values(col("attributes")))

        # Working with structs
        if name_col:
            struct_df = df \
                .withColumn("customer_info",
                           struct(col(name_col).alias("full_name"),
                                  col("id").alias("customer_id"))) \
                .withColumn("customer_name", col("customer_info.full_name"))
        else:
            struct_df = df.withColumn("customer_info", struct(col("id").alias("customer_id")))

        print("Complex data types enable nested data processing")
        return array_df, exploded_df, map_df, struct_df

    def advanced_string_transformations(self, df):
        """Advanced string processing and text transformations"""
        print("\n=== Advanced String Transformations ===")

        name_col = "product_name" if "product_name" in df.columns else ("name" if "name" in df.columns else None)
        amount_col = "sales" if "sales" in df.columns else ("amount" if "amount" in df.columns else None)

        if not name_col:
            # If no suitable name column, add a synthetic column to operate on
            df = df.withColumn("product_name", lit("N/A"))
            name_col = "product_name"

        # String cleaning and normalization
        cleaned_df = df \
            .withColumn("name_cleaned",
                       trim(lower(regexp_replace(col(name_col), "[^a-zA-Z0-9\\s]", "")))) \
            .withColumn("name_standardized",
                       regexp_replace(col("name_cleaned"), "\\s+", " "))

        # Text extraction with regex
        extracted_df = cleaned_df \
            .withColumn("name_parts", split(col("name_standardized"), " ")) \
            .withColumn("first_name", col("name_parts")[0]) \
            .withColumn("last_name", col("name_parts")[size(col("name_parts")) - 1])

        # Advanced string functions
        string_ops_df = extracted_df \
            .withColumn("name_length", length(col(name_col))) \
            .withColumn("name_hash", hash(col(name_col))) \
            .withColumn("name_base64", base64(col(name_col).cast("binary"))) \
            .withColumn("name_soundex", soundex(col(name_col)))

        # JSON processing
        if not amount_col:
            df = df.withColumn("sales", lit(0.0).cast("double"))
            amount_col = "sales"

        json_df = df \
            .withColumn("json_data",
                       to_json(struct(col("id"), col(name_col).alias("name"), col(amount_col).alias("amount")))) \
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

        # Ranking and analytical functions
        ranking_df = df \
            .withColumn("row_number", row_number().over(ordered_window)) \
            .withColumn("rank", rank().over(ordered_window)) \
            .withColumn("dense_rank", dense_rank().over(ordered_window)) \
            .withColumn("percent_rank", percent_rank().over(ordered_window)) \
            .withColumn("ntile", ntile(4).over(ordered_window))

        # Lag/Lead operations
        lag_lead_df = ranking_df \
            .withColumn("prev_sales", lag("sales", 1).over(ordered_window)) \
            .withColumn("next_sales", lead("sales", 1).over(ordered_window)) \
            .withColumn("sales_change", col("sales") - col("prev_sales")) \
            .withColumn("sales_change_pct",
                       (col("sales") - col("prev_sales")) / col("prev_sales") * 100)

        # Moving aggregations
        moving_agg_df = lag_lead_df \
            .withColumn("moving_avg_3", avg("sales").over(
                ordered_window.rowsBetween(-2, 0))) \
            .withColumn("moving_sum_7", sum("sales").over(
                ordered_window.rowsBetween(-6, 0))) \
            .withColumn("running_total", sum("sales").over(
                ordered_window.rowsBetween(Window.unboundedPreceding, 0)))

        # First/Last value operations
        first_last_df = moving_agg_df \
            .withColumn("first_sales", first("sales").over(ordered_window)) \
            .withColumn("last_sales", last("sales").over(ordered_window)) \
            .withColumn("min_sales", F.min("sales").over(partition_window)) \
            .withColumn("max_sales", F.max("sales").over(partition_window))

        return ranking_df, lag_lead_df, moving_agg_df, first_last_df

    # Backward-compatible method name expected by tests
    def window_function_mastery(self, df):
        return self.advanced_window_operations(df)

    def pivot_and_unpivot_operations(self, df):
        """Demonstrate pivot, unpivot (melt), and crosstab operations using available columns"""
        print("\n=== Pivot and Unpivot Operations ===")

        # Pivot: sales by region with categories as columns
        pivoted_df = df.groupBy("region").pivot("category").agg(sum("sales").alias("total_sales"))

        # Unpivot: convert pivoted wide columns back to long format (category, total_sales)
        category_cols = [c for c in pivoted_df.columns if c != "region"]
        if category_cols:
            exprs = " , ".join([f"'{c}', `{c}`" for c in category_cols])
            unpivot_df = pivoted_df.select(
                col("region"),
                expr(f"stack({len(category_cols)}, {exprs}) as (category, total_sales)")
            ).where(col("total_sales").isNotNull())
        else:
            unpivot_df = df.select("region", col("category"), col("sales").alias("total_sales"))

        # Crosstab between category and region
        crosstab_df = df.crosstab("category", "region")

        return pivoted_df, unpivot_df, crosstab_df

    def data_quality_transformations(self, df):
        """Data quality checks and cleansing transformations"""
        print("\n=== Data Quality Transformations ===")

        name_col = "product_name" if "product_name" in df.columns else ("name" if "name" in df.columns else None)
        amount_col = "sales" if "sales" in df.columns else ("amount" if "amount" in df.columns else None)

        # Null handling strategies
        base_df = df
        if name_col is None:
            base_df = base_df.withColumn("product_name", lit(None).cast("string"))
            name_col = "product_name"
        if amount_col is None:
            base_df = base_df.withColumn("sales", lit(None).cast("double"))
            amount_col = "sales"

        null_handled_df = base_df \
            .withColumn("name_cleaned", coalesce(col(name_col), lit("UNKNOWN"))) \
            .withColumn("amount_cleaned",
                       when(col(amount_col).isNull() | (col(amount_col) < 0), 0)
                       .otherwise(col(amount_col))) \
            .fillna({"category": "OTHER", "status": "PENDING"})

        # Duplicate detection
        duplicate_check_df = null_handled_df \
            .withColumn("row_hash", hash(col(name_col), col("category"))) \
            .withColumn("duplicate_count",
                       count("*").over(Window.partitionBy("row_hash"))) \
            .withColumn("is_duplicate", col("duplicate_count") > 1)

        # Outlier detection using IQR method
        stats_row = duplicate_check_df \
            .select(
                expr(f"percentile_approx({amount_col}, 0.25)").alias("q1"),
                expr(f"percentile_approx({amount_col}, 0.75)").alias("q3")
            ).collect()[0]

        iqr = stats_row["q3"] - stats_row["q1"]
        lower_bound = stats_row["q1"] - 1.5 * iqr
        upper_bound = stats_row["q3"] + 1.5 * iqr

        outlier_df = duplicate_check_df \
            .withColumn("is_outlier",
                       (col(amount_col) < lower_bound) | (col(amount_col) > upper_bound)) \
            .withColumn("amount_capped",
                       when(col(amount_col) > upper_bound, upper_bound)
                       .when(col(amount_col) < lower_bound, lower_bound)
                       .otherwise(col(amount_col)))

        # Data validation rules
        validated_df = outlier_df \
            .withColumn("validation_errors", array()) \
            .withColumn("validation_errors",
                       when(col(name_col).isNull() | (col(name_col) == ""),
                            array_union(col("validation_errors"), array(lit("MISSING_NAME"))))
                       .otherwise(col("validation_errors"))) \
            .withColumn("validation_errors",
                       when(col(amount_col) < 0,
                            array_union(col("validation_errors"), array(lit("NEGATIVE_AMOUNT"))))
                       .otherwise(col("validation_errors"))) \
            .withColumn("is_valid", size(col("validation_errors")) == 0)

        return null_handled_df, duplicate_check_df, outlier_df, validated_df

    def performance_optimized_transformations(self, df):
        """Performance-optimized transformation patterns"""
        print("\n=== Performance-Optimized Transformations ===")

        amount_col = "sales" if "sales" in df.columns else ("amount" if "amount" in df.columns else None)
        status_col = "status" if "status" in df.columns else None

        # Efficient filtering and selection
        optimized_df = df
        if amount_col:
            optimized_df = optimized_df.filter(col(amount_col) > 0)
        if status_col:
            optimized_df = optimized_df.filter(col(status_col).isin(["active", "pending", "completed", "pending"]))

        select_cols = [c for c in ["id", "product_name", amount_col, "category"] if c and c in optimized_df.columns]
        if select_cols:
            optimized_df = optimized_df.select(*select_cols)

        # Efficient joins with broadcast
        if "category" in df.columns:
            small_lookup = df.select("category").distinct() \
                            .withColumn("category_type",
                                       when(col("category").like("%Electronics%"), "premium")
                                       .otherwise("standard"))
            joined_df = optimized_df.join(broadcast(small_lookup), "category") if "category" in optimized_df.columns else optimized_df
        else:
            joined_df = optimized_df

        # Columnar operations (vectorized)
        if amount_col and amount_col in joined_df.columns and "category" in joined_df.columns:
            window_by_cat = Window.partitionBy("category")
            vectorized_df = joined_df \
                .withColumn("amount_squared", col(amount_col) * col(amount_col)) \
                .withColumn("log_amount", log(col(amount_col) + 1)) \
                .withColumn("normalized_amount",
                           (col(amount_col) - avg(col(amount_col)).over(window_by_cat)) /
                           stddev(col(amount_col)).over(window_by_cat))
        else:
            vectorized_df = joined_df

        # Cache intermediate results
        vectorized_df.cache()

        # Multiple transformations on cached data
        result1 = vectorized_df.groupBy("category").sum(amount_col) if amount_col and "category" in vectorized_df.columns else vectorized_df
        result2 = vectorized_df.filter(col("amount_squared") > 1000000) if "amount_squared" in vectorized_df.columns else vectorized_df

        return optimized_df, joined_df, vectorized_df, result1, result2

# Example usage remains for manual run
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
