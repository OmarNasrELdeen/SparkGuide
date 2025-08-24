"""
Spark Query Optimization and Performance Tuning
This module demonstrates advanced query optimization techniques for faster ETL processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

class SparkQueryOptimizer:
    def __init__(self, app_name="SparkQueryOptimizer"):
        """Initialize Spark with query optimization settings"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.cbo.enabled", "true") \
            .config("spark.sql.statistics.histogram.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

    def enable_cost_based_optimization(self, df, table_name):
        """Enable and configure Cost-Based Optimization (CBO)"""
        # Create temporary view
        df.createOrReplaceTempView(table_name)

        # Analyze table statistics
        self.spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
        self.spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

        # Show statistics
        stats = self.spark.sql(f"DESCRIBE EXTENDED {table_name}")
        print(f"Table statistics for {table_name}:")
        stats.filter(col("col_name").isin(["Statistics", "numFiles", "numRows", "sizeInBytes"])).show()

        return df

    def optimize_predicates_pushdown(self, server, database, username, password, table_name):
        """Demonstrate predicate pushdown optimization"""
        url = f"jdbc:sqlserver://{server}:1433;databaseName={database}"
        properties = {
            "user": username,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "pushDownPredicate": "true",  # Enable predicate pushdown
            "pushDownAggregate": "true"   # Enable aggregate pushdown
        }

        # Read with predicate that will be pushed down to database
        df = self.spark.read \
            .jdbc(url=url, table=table_name, properties=properties) \
            .filter(col("status") == "active") \
            .filter(col("created_date") >= "2024-01-01")

        print("Predicates pushed down to database level - reduces data transfer")
        return df

    def optimize_joins_strategies(self, large_df, medium_df, small_df):
        """Demonstrate different join strategies and optimization"""
        print("\n=== Join Optimization Strategies ===")

        # 1. Broadcast Hash Join (for small tables < 10MB)
        broadcast_join = large_df.join(broadcast(small_df), "key")
        print("Broadcast Hash Join: Best for small dimension tables")

        # 2. Sort Merge Join (for large tables)
        # Spark will automatically choose this for large-large joins
        sort_merge_join = large_df.join(medium_df, "key")
        print("Sort Merge Join: Default for large table joins")

        # 3. Bucketed joins (pre-partitioned data)
        # This requires writing data with buckets first
        print("Bucketed Join: Best for frequently joined tables")

        # 4. Join ordering optimization
        # Join smaller tables first, then larger ones
        optimized_join = small_df \
            .join(medium_df, "key") \
            .join(large_df, "key")

        return optimized_join

    def optimize_aggregations(self, df):
        """Optimize aggregation operations"""
        print("\n=== Aggregation Optimization ===")

        # 1. Use partial aggregations
        # Group by higher cardinality columns first
        optimized_agg1 = df \
            .groupBy("high_cardinality_col", "low_cardinality_col") \
            .agg(sum("value").alias("total_value"),
                 count("*").alias("count"),
                 avg("value").alias("avg_value"))

        # 2. Use approximate aggregations for large datasets
        approx_agg = df \
            .groupBy("category") \
            .agg(approx_count_distinct("user_id").alias("unique_users"),
                 expr("percentile_approx(value, 0.5)").alias("median_value"))

        # 3. Cache intermediate results for multiple aggregations
        df.cache()

        # Multiple aggregations on same data
        agg1 = df.groupBy("region").sum("sales")
        agg2 = df.groupBy("region").avg("sales")
        agg3 = df.groupBy("region").count()

        return optimized_agg1, approx_agg

    def optimize_window_functions(self, df):
        """Optimize window function operations"""
        from pyspark.sql.window import Window

        # Define optimized window specifications
        # Partition by columns with good cardinality
        window_spec = Window \
            .partitionBy("category") \
            .orderBy("date") \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)

        # Use window functions efficiently
        result = df \
            .withColumn("running_total", sum("value").over(window_spec)) \
            .withColumn("row_number", row_number().over(window_spec)) \
            .withColumn("rank", dense_rank().over(window_spec))

        # Optimize by limiting partitions in window
        limited_window = Window \
            .partitionBy("category") \
            .orderBy("date") \
            .rowsBetween(-7, 0)  # Only look at last 7 rows

        result = result.withColumn("weekly_avg", avg("value").over(limited_window))

        return result

    def optimize_column_operations(self, df):
        """Optimize column-level operations"""
        # 1. Select only needed columns early
        selected_df = df.select("id", "name", "value", "date")

        # 2. Use built-in functions instead of UDFs when possible
        optimized_df = selected_df \
            .withColumn("value_squared", col("value") * col("value")) \
            .withColumn("name_upper", upper(col("name"))) \
            .withColumn("year", year(col("date")))

        # 3. Chain transformations efficiently
        final_df = optimized_df \
            .filter(col("value") > 0) \
            .filter(col("year") >= 2024) \
            .select("id", "name_upper", "value_squared")

        return final_df

    def optimize_sql_queries(self, df, table_name):
        """Optimize SQL query patterns"""
        df.createOrReplaceTempView(table_name)

        # 1. Use EXISTS instead of IN for better performance
        optimized_exists = self.spark.sql(f"""
            SELECT * FROM {table_name} t1
            WHERE EXISTS (
                SELECT 1 FROM {table_name} t2 
                WHERE t2.category = t1.category AND t2.status = 'active'
            )
        """)

        # 2. Use CASE WHEN efficiently
        case_when_optimized = self.spark.sql(f"""
            SELECT *,
                CASE 
                    WHEN value > 1000 THEN 'high'
                    WHEN value > 100 THEN 'medium'
                    ELSE 'low'
                END as value_category
            FROM {table_name}
        """)

        # 3. Use subqueries efficiently
        subquery_optimized = self.spark.sql(f"""
            WITH ranked_data AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rn
                FROM {table_name}
            )
            SELECT * FROM ranked_data WHERE rn <= 10
        """)

        return optimized_exists, case_when_optimized, subquery_optimized

    def cache_strategy_optimization(self, df):
        """Implement optimal caching strategies"""
        # 1. Cache expensive intermediate results
        expensive_df = df \
            .join(df.groupBy("category").agg(avg("value").alias("avg_value")), "category") \
            .withColumn("deviation", abs(col("value") - col("avg_value")))

        # Cache before multiple actions
        expensive_df.cache()

        # Multiple operations on cached data
        count_result = expensive_df.count()
        filtered_result = expensive_df.filter(col("deviation") > 10)

        # 2. Use appropriate storage levels
        from pyspark import StorageLevel

        # For frequently accessed data
        df.persist(StorageLevel.MEMORY_AND_DISK_SER)

        # For less frequent access
        # df.persist(StorageLevel.DISK_ONLY)

        return expensive_df

# Example usage
if __name__ == "__main__":
    optimizer = SparkQueryOptimizer()

    # Create sample data
    data = [(i, f"category_{i%10}", f"name_{i}", i * 10, f"2024-{(i%12)+1:02d}-01")
            for i in range(100000)]
    df = optimizer.spark.createDataFrame(data, ["id", "category", "name", "value", "date"])
    df = df.withColumn("date", to_date(col("date")))

    # Enable CBO
    optimizer.enable_cost_based_optimization(df, "sample_table")

    # Optimize aggregations
    agg1, agg2 = optimizer.optimize_aggregations(df)
    agg1.show(10)

    # Optimize window functions
    windowed_df = optimizer.optimize_window_functions(df)
    windowed_df.show(10)

    optimizer.spark.stop()
