# Spark Grouping Strategies Documentation

## Overview
The `spark_grouping_strategies.py` module demonstrates optimal grouping and aggregation strategies for different scenarios, including when to use groupBy vs window functions, handling skewed data, and advanced aggregation techniques.

## Class: SparkGroupingStrategies

### Initialization

```python
grouper = SparkGroupingStrategies(app_name="SparkGrouping")
```

#### Spark Configuration Options

| Configuration | Value | Purpose | Performance Impact |
|---------------|-------|---------|-------------------|
| `spark.sql.adaptive.enabled` | `"true"` | Enable Adaptive Query Execution | Better aggregation optimization |
| `spark.sql.adaptive.skewJoin.enabled` | `"true"` | Handle skewed data in joins | Better performance with skewed aggregations |
| `spark.sql.adaptive.coalescePartitions.enabled` | `"true"` | Auto-merge small result partitions | Fewer small files after grouping |

## Method: when_to_group_vs_window()

### Purpose
Demonstrates when to use `groupBy` vs `window functions` based on the desired output and performance requirements.

### Parameters
- `df`: DataFrame to analyze

### GroupBy vs Window Functions Decision Matrix

| Requirement | GroupBy | Window Functions | Best Choice |
|-------------|---------|------------------|-------------|
| **Reduce data size** | ✅ Aggregates to fewer rows | ❌ Keeps all rows | GroupBy |
| **Keep detail data** | ❌ Loses individual rows | ✅ Preserves all rows | Window Functions |
| **Final reporting** | ✅ Summary results | ❌ Too much detail | GroupBy |
| **Ranking/Percentiles** | ❌ Can't rank within groups | ✅ Perfect for ranking | Window Functions |
| **Running totals** | ❌ No running calculations | ✅ Over partitions | Window Functions |
| **Memory efficiency** | ✅ Smaller result sets | ❌ Larger result sets | GroupBy |

**Example Usage - GroupBy for Aggregation:**
```python
# Use groupBy when you need final aggregated results
sales_df = spark.read.table("sales_transactions")

# GroupBy approach - reduces data to summary
grouped_result = sales_df \
    .groupBy("product_category", "region") \
    .agg(
        sum("sales_amount").alias("total_sales"),
        count("*").alias("transaction_count"),
        avg("sales_amount").alias("avg_sales"),
        countDistinct("customer_id").alias("unique_customers")
    )

# Result: Much smaller dataset with summary statistics
# Input: 10M rows → Output: 50 rows (category × region combinations)
```

**Example Usage - Window Functions for Detail + Aggregation:**
```python
from pyspark.sql.window import Window

# Use window functions when you need both detail and aggregated data
window_spec = Window.partitionBy("product_category", "region")

windowed_result = sales_df \
    .withColumn("category_total_sales", sum("sales_amount").over(window_spec)) \
    .withColumn("category_avg_sales", avg("sales_amount").over(window_spec)) \
    .withColumn("sales_rank", dense_rank().over(
        Window.partitionBy("product_category", "region")
               .orderBy(desc("sales_amount"))
    )) \
    .withColumn("running_total", sum("sales_amount").over(
        Window.partitionBy("product_category", "region")
               .orderBy("transaction_date")
               .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ))

# Result: Same number of rows with additional analytical columns
# Input: 10M rows → Output: 10M rows (with extra columns)
```

### Performance Considerations

| Aspect | GroupBy | Window Functions |
|--------|---------|------------------|
| **Memory Usage** | Low (aggregated results) | High (all rows kept) |
| **Processing Speed** | Fast (fewer operations) | Slower (more operations) |
| **Network I/O** | Low (smaller results) | High (larger results) |
| **Use Case** | Final reports, dashboards | Detailed analysis, ranking |

## Method: optimize_multi_level_grouping()

### Purpose
Optimizes multi-level grouping operations using `ROLLUP` and `CUBE` for hierarchical aggregations.

### Traditional vs Optimized Approach

#### Traditional Approach (Inefficient)
```python
# Multiple separate grouping operations (inefficient)
region_totals = df.groupBy("region").agg(sum("sales").alias("sales"))
category_totals = df.groupBy("category").agg(sum("sales").alias("sales"))
region_category_totals = df.groupBy("region", "category").agg(sum("sales").alias("sales"))
overall_total = df.agg(sum("sales").alias("sales"))

# Problems:
# - Multiple passes over data
# - Separate shuffle operations
# - Higher I/O and computation costs
```

#### Optimized Approach with ROLLUP
```python
# Single operation with hierarchical aggregation
rollup_result = df \
    .rollup("region", "category") \
    .agg(
        sum("sales").alias("total_sales"),
        count("*").alias("count"),
        avg("sales").alias("avg_sales")
    ) \
    .orderBy("region", "category")

# Results include:
# - Grand total (region=null, category=null)
# - Region totals (region=value, category=null)  
# - Region+Category details (region=value, category=value)
```

#### CUBE for All Combinations
```python
# All possible combinations of grouping columns
cube_result = df \
    .cube("region", "category", "product_type") \
    .agg(sum("sales").alias("total_sales")) \
    .filter(col("total_sales").isNotNull())

# Results include all combinations:
# - (null, null, null) - Grand total
# - (region, null, null) - By region only
# - (null, category, null) - By category only  
# - (null, null, product_type) - By product type only
# - (region, category, null) - By region + category
# - (region, null, product_type) - By region + product type
# - (null, category, product_type) - By category + product type
# - (region, category, product_type) - Full detail
```

**Example Usage:**
```python
# Sample sales data
sales_data = [
    ("North", "Electronics", "Laptop", 1500),
    ("North", "Electronics", "Phone", 800),
    ("South", "Clothing", "Shirt", 50),
    ("South", "Clothing", "Pants", 80)
]

sales_df = spark.createDataFrame(sales_data, 
    ["region", "category", "product_type", "sales"])

# Get hierarchical aggregations
rollup_result, cube_result = grouper.optimize_multi_level_grouping(sales_df)

# Rollup result:
# +------+----------+-----------+-----+
# |region|  category|total_sales|count|
# +------+----------+-----------+-----+
# | North|Electronics|       2300|    2|
# | South|  Clothing |        130|    2|
# | North|      null|       2300|    2|
# | South|      null|        130|    2|
# |  null|      null|       2430|    4|
# +------+----------+-----------+-----+
```

### ROLLUP vs CUBE Decision Guide

| Use Case | ROLLUP | CUBE | Reason |
|----------|--------|------|--------|
| **Hierarchical data** | ✅ Perfect | ❌ Too many combinations | Natural hierarchy (region → category) |
| **All combinations needed** | ❌ Limited | ✅ Perfect | Need every possible grouping |
| **Time-series with geography** | ✅ Year → Quarter → Month | ❌ Overkill | Temporal hierarchy |
| **Small dimension count** | ✅ Good | ✅ Good | Both work well |
| **Many dimensions (>3)** | ✅ Manageable | ❌ Exponential growth | Cube creates too many combinations |

## Method: handle_skewed_grouping()

### Purpose
Handles data skew in grouping operations using salting techniques to improve performance.

### Parameters
- `df`: DataFrame with skewed data
- `skewed_column`: Column that has skewed distribution

### How Data Skew Affects Grouping
- **Problem**: Some groups have much more data than others
- **Impact**: Few tasks process most data while others are idle
- **Symptoms**: Long-running tasks, uneven progress, poor cluster utilization

### Skew Detection
```python
# Analyze data distribution
skew_analysis = df \
    .groupBy("customer_id") \
    .count() \
    .orderBy(desc("count"))

skew_analysis.show(10)
# +----------+-------+
# |customer_id| count|
# +----------+-------+
# |    CUST001|1500000| ← Highly skewed customer
# |    CUST002|1200000| ← Highly skewed customer  
# |    CUST003|    150| ← Normal customer
# |    CUST004|    200| ← Normal customer
# +----------+-------+

# Calculate skew ratio
stats = skew_analysis.agg(
    max("count").alias("max_count"),
    avg("count").alias("avg_count")
).collect()[0]

skew_ratio = stats["max_count"] / stats["avg_count"]
# If skew_ratio > 5, consider skew handling
```

### Salting Technique for Skew Handling
```python
# Step 1: Add salt to distribute skewed keys
salted_df = df \
    .withColumn("salt", (rand() * 10).cast("int")) \
    .withColumn("salted_key", concat(col("customer_id"), lit("_"), col("salt")))

# Step 2: Group by salted key (distributes skewed data)
salted_grouped = salted_df \
    .groupBy("salted_key") \
    .agg(sum("sales").alias("partial_sum"))

# Step 3: Remove salt and re-aggregate
final_result = salted_grouped \
    .withColumn("customer_id", regexp_replace(col("salted_key"), "_\\d+$", "")) \
    .groupBy("customer_id") \
    .agg(sum("partial_sum").alias("total_sales"))
```

**Example Usage:**
```python
# Sample skewed customer data
skewed_data = []
# Create highly skewed data - one customer with 80% of transactions
for i in range(100000):
    if i < 80000:
        customer_id = "MEGA_CUSTOMER"  # 80% of data
    else:
        customer_id = f"CUST_{i}"      # 20% of data
    
    skewed_data.append((customer_id, f"PRODUCT_{i%100}", i * 10))

skewed_df = spark.createDataFrame(skewed_data, 
    ["customer_id", "product", "sales"])

# Handle skew using salting
deskewed_result = grouper.handle_skewed_grouping(skewed_df, "customer_id")

# Performance improvement:
# - Before: One task processes 80K records, others process few
# - After: Load distributed across 10 tasks (salt factor)
```

### Skew Handling Strategies

| Strategy | When to Use | Performance Gain | Complexity |
|----------|-------------|------------------|------------|
| **Salting** | High skew on grouping key | 5-20x faster | Medium |
| **Pre-filtering** | Can filter skewed values | 2-10x faster | Low |
| **Separate processing** | Few highly skewed keys | 3-15x faster | High |
| **Bucketing** | Repeated skewed grouping | 10-50x faster | High (requires pre-processing) |

## Method: optimize_grouping_with_filters()

### Purpose
Optimizes grouping operations by applying filters before and after aggregation.

### Filter Optimization Strategies

#### 1. Pre-Aggregation Filtering
```python
# Apply filters BEFORE grouping to reduce data size
optimized_grouped = df \
    .filter(col("status") == "completed") \      # Reduces input rows
    .filter(col("amount") > 0) \                 # Removes invalid data
    .filter(col("transaction_date") >= "2024-01-01") \  # Time-based filter
    .groupBy("customer_segment", "product_category") \
    .agg(
        sum("amount").alias("total_amount"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("amount").alias("avg_amount")
    )
```

#### 2. Post-Aggregation Filtering (HAVING equivalent)
```python
# Apply filters AFTER grouping for business rules
final_result = optimized_grouped \
    .filter(col("total_amount") > 10000) \       # Minimum revenue threshold
    .filter(col("unique_customers") > 5) \       # Minimum customer count
    .filter(col("avg_amount") > 100)             # Minimum average order
```

**Example Usage:**
```python
# Sample transaction data with various statuses
transaction_data = [
    ("Premium", "Electronics", "completed", 1500, "CUST001"),
    ("Premium", "Electronics", "pending", 800, "CUST002"),
    ("Standard", "Clothing", "completed", 50, "CUST003"),
    ("Standard", "Clothing", "cancelled", 75, "CUST004"),
]

transactions_df = spark.createDataFrame(transaction_data,
    ["customer_segment", "product_category", "status", "amount", "customer_id"])

# Apply filter optimization
filtered_result = grouper.optimize_grouping_with_filters(transactions_df)

# Performance benefits:
# - Pre-filtering: Reduces shuffle data by 60-90%
# - Post-filtering: Applies business rules efficiently
```

### Filter Placement Strategy

| Filter Type | Placement | Reason | Performance Impact |
|-------------|-----------|--------|-------------------|
| **Data Quality** | Before grouping | Remove bad data early | 50-80% less shuffle data |
| **Time Range** | Before grouping | Reduce input size | 70-95% less processing |
| **Business Rules** | After grouping | Apply to aggregated results | Minimal impact |
| **Threshold Filters** | After grouping | Based on aggregated values | Clean final results |

## Method: advanced_aggregation_functions()

### Purpose
Demonstrates advanced aggregation functions for complex analytical requirements.

### Advanced Aggregation Types

#### 1. Collection Aggregations
```python
# Collect values into lists and sets
collection_agg = df \
    .groupBy("category") \
    .agg(
        collect_list("product_name").alias("all_products"),        # All values (with duplicates)
        collect_set("product_name").alias("unique_products"),      # Unique values only
        sort_array(collect_set("product_name")).alias("sorted_products")  # Sorted unique values
    )
```

#### 2. Statistical Aggregations
```python
# Advanced statistical measures
stats_agg = df \
    .groupBy("region") \
    .agg(
        stddev("sales").alias("sales_stddev"),          # Standard deviation
        variance("sales").alias("sales_variance"),       # Variance
        skewness("sales").alias("sales_skewness"),       # Skewness (distribution shape)
        kurtosis("sales").alias("sales_kurtosis"),       # Kurtosis (tail heaviness)
        corr("sales", "quantity").alias("sales_qty_corr") # Correlation
    )
```

#### 3. Percentile Aggregations
```python
# Percentile and quantile calculations
percentile_agg = df \
    .groupBy("product_category") \
    .agg(
        expr("percentile_approx(sales, 0.25)").alias("q1"),         # First quartile
        expr("percentile_approx(sales, 0.5)").alias("median"),      # Median
        expr("percentile_approx(sales, 0.75)").alias("q3"),         # Third quartile
        expr("percentile_approx(sales, 0.95)").alias("p95"),        # 95th percentile
        expr("percentile_approx(sales, array(0.1, 0.9))").alias("p10_p90")  # Multiple percentiles
    )
```

#### 4. Custom Aggregations
```python
# Complex business logic aggregations
custom_agg = df \
    .groupBy("customer_tier") \
    .agg(
        # Conditional aggregations
        sum(when(col("amount") > 1000, col("amount")).otherwise(0)).alias("high_value_sales"),
        count(when(col("status") == "completed", 1)).alias("completed_orders"),
        
        # Multi-condition aggregations
        avg(when(col("product_type") == "premium", col("amount"))).alias("premium_avg"),
        
        # Combined metrics
        (sum("amount") / countDistinct("customer_id")).alias("revenue_per_customer")
    )
```

**Example Usage:**
```python
# Sample product sales data
product_data = [
    ("Electronics", "Laptop Pro", 1500, "North"),
    ("Electronics", "Phone X", 800, "North"),
    ("Electronics", "Tablet", 600, "South"),
    ("Clothing", "Shirt", 50, "North"),
    ("Clothing", "Pants", 80, "South"),
    ("Clothing", "Jacket", 120, "North")
]

products_df = spark.createDataFrame(product_data,
    ["category", "product_name", "sales", "region"])

# Apply advanced aggregations
list_agg, stats_agg, percentile_agg = grouper.advanced_aggregation_functions(products_df)

# Results show:
# - Collection aggregations: Product lists per category
# - Statistical measures: Distribution analysis per region  
# - Percentiles: Sales distribution quartiles per category
```

### Aggregation Function Performance

| Function Type | Performance | Use Case | Memory Usage |
|---------------|-------------|----------|--------------|
| **Basic** (sum, count, avg) | Fastest | Standard reporting | Low |
| **Statistical** (stddev, variance) | Medium | Data analysis | Medium |
| **Percentiles** | Slower | Distribution analysis | High |
| **Collections** | Slowest | Detailed listings | Very High |

## Method: optimize_grouping_for_time_series()

### Purpose
Optimizes grouping for time-series data with temporal aggregations and moving windows.

### Parameters
- `df`: DataFrame with time-series data
- `date_column`: Date column for temporal grouping

### Time-Series Grouping Strategies

#### 1. Hierarchical Time Grouping
```python
# Add time-based grouping columns
time_enhanced_df = df \
    .withColumn("year", year(col(date_column))) \
    .withColumn("quarter", quarter(col(date_column))) \
    .withColumn("month", month(col(date_column))) \
    .withColumn("week", weekofyear(col(date_column))) \
    .withColumn("day_of_week", dayofweek(col(date_column)))
```

#### 2. Period-Based Aggregations
```python
# Daily aggregations
daily_agg = time_enhanced_df \
    .groupBy("year", "month", dayofmonth(col(date_column)).alias("day")) \
    .agg(
        sum("sales").alias("daily_sales"),
        count("*").alias("daily_transactions"),
        countDistinct("customer_id").alias("daily_customers")
    )

# Weekly aggregations  
weekly_agg = time_enhanced_df \
    .groupBy("year", "week") \
    .agg(
        sum("sales").alias("weekly_sales"),
        avg("sales").alias("weekly_avg_sales")
    )

# Monthly aggregations with year-over-year comparison
monthly_agg = time_enhanced_df \
    .groupBy("year", "month") \
    .agg(sum("sales").alias("monthly_sales")) \
    .withColumn("prev_year_month", 
               lag("monthly_sales", 12).over(
                   Window.orderBy("year", "month")
               )) \
    .withColumn("yoy_growth", 
               ((col("monthly_sales") - col("prev_year_month")) / 
                col("prev_year_month") * 100))
```

#### 3. Moving Window Aggregations
```python
from pyspark.sql.window import Window

# Moving averages and trends
moving_window = Window.orderBy("year", "month").rowsBetween(-2, 0)

monthly_with_trends = monthly_agg \
    .withColumn("3_month_moving_avg", 
               avg("monthly_sales").over(moving_window)) \
    .withColumn("sales_trend",
               (col("monthly_sales") - col("3_month_moving_avg")) / 
               col("3_month_moving_avg") * 100)
```

**Example Usage:**
```python
# Sample time-series sales data
from datetime import datetime, timedelta
import random

time_series_data = []
base_date = datetime(2024, 1, 1)

for i in range(1000):
    date = base_date + timedelta(days=random.randint(0, 365))
    sales = random.randint(100, 2000)
    customer_id = f"CUST_{random.randint(1, 100)}"
    
    time_series_data.append((date, sales, customer_id))

ts_df = spark.createDataFrame(time_series_data, 
    ["sale_date", "sales", "customer_id"])

# Apply time-series grouping optimizations
daily, weekly, monthly = grouper.optimize_grouping_for_time_series(ts_df, "sale_date")

# Results provide:
# - Daily: Day-by-day analysis
# - Weekly: Weekly patterns and trends  
# - Monthly: Monthly summaries with moving averages
```

### Time-Series Aggregation Patterns

| Time Granularity | Use Case | Performance | Storage |
|------------------|----------|-------------|---------|
| **Hourly** | Real-time monitoring | Fast | High |
| **Daily** | Operational reporting | Medium | Medium |  
| **Weekly** | Trend analysis | Medium | Low |
| **Monthly** | Strategic planning | Slow | Very Low |
| **Quarterly** | Executive reporting | Slow | Minimal |

## Complete Usage Examples

### Example 1: E-commerce Analytics Pipeline
```python
# Initialize grouping strategies
grouper = SparkGroupingStrategies("EcommerceAnalytics")

# Read transaction data
transactions_df = spark.read.table("ecommerce_transactions")

# Step 1: Handle skewed customers (few customers with many orders)
balanced_df = grouper.handle_skewed_grouping(transactions_df, "customer_id")

# Step 2: Multi-level analysis with ROLLUP
rollup_analysis = transactions_df \
    .rollup("region", "product_category", "customer_segment") \
    .agg(
        sum("order_amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("order_amount").alias("avg_order_value")
    )

# Step 3: Advanced aggregations for insights
advanced_metrics = grouper.advanced_aggregation_functions(transactions_df)

# Step 4: Time-series analysis for trends
daily, weekly, monthly = grouper.optimize_grouping_for_time_series(
    transactions_df, "order_date"
)
```

### Example 2: Customer Behavior Analysis
```python
# Compare groupBy vs window functions for customer analysis
customer_df = spark.read.table("customer_interactions")

# GroupBy approach: Customer summaries
customer_summary, customer_detail = grouper.when_to_group_vs_window(customer_df)

# Customer summary (aggregated):
# customer_id | total_interactions | avg_session_duration | ...

# Customer detail (with ranking):  
# customer_id | interaction_date | session_duration | customer_rank | ...

# Choose based on downstream needs:
# - Dashboards/Reports: Use customer_summary
# - Detailed Analysis: Use customer_detail
```

This comprehensive documentation covers all grouping strategies with practical examples and performance optimization techniques for analytical workloads.
