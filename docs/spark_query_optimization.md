# Spark Query Optimization Documentation

## Overview
The `spark_query_optimization.py` module demonstrates advanced query optimization techniques for faster analytical processing and ETL performance through Cost-Based Optimization, predicate pushdown, and join strategies.

## Class: SparkQueryOptimizer

### Initialization

```python
optimizer = SparkQueryOptimizer(app_name="SparkQueryOptimizer")
```

#### Advanced Spark Configuration Options

| Configuration | Value | Purpose | Performance Impact |
|---------------|-------|---------|-------------------|
| `spark.sql.adaptive.enabled` | `"true"` | Enable Adaptive Query Execution | 20-50% query improvement |
| `spark.sql.adaptive.coalescePartitions.enabled` | `"true"` | Auto-merge small partitions | Reduces task overhead |
| `spark.sql.adaptive.skewJoin.enabled` | `"true"` | Handle skewed joins | 2-10x faster skewed joins |
| `spark.sql.cbo.enabled` | `"true"` | Enable Cost-Based Optimization | 30-80% query improvement |
| `spark.sql.statistics.histogram.enabled` | `"true"` | Enable column histograms for CBO | Better join ordering |
| `spark.sql.adaptive.localShuffleReader.enabled` | `"true"` | Optimize shuffle reads | Reduced network I/O |
| `spark.serializer` | `"KryoSerializer"` | Fast serialization | Better performance |

### Cost-Based Optimization (CBO) Benefits
- **Join Reordering**: Automatically chooses optimal join order
- **Join Strategy Selection**: Picks best join algorithm (broadcast, sort-merge, etc.)
- **Cardinality Estimation**: Better resource allocation
- **Filter Selectivity**: Pushes down most selective filters first

## Method: enable_cost_based_optimization()

### Purpose
Enables and configures Cost-Based Optimization by collecting table statistics for better query planning.

### Parameters
- `df`: DataFrame to analyze
- `table_name`: Name for temporary view

### How CBO Works
1. **Statistics Collection**: Analyzes table and column statistics
2. **Cardinality Estimation**: Estimates result sizes at each step
3. **Cost Calculation**: Computes cost for different execution plans
4. **Plan Selection**: Chooses lowest-cost execution plan

### Statistics Collected
- **Table Statistics**: Row count, file count, total size
- **Column Statistics**: Min/max values, null count, distinct count
- **Histograms**: Value distribution for better selectivity estimation

**Example Usage:**
```python
# Enable CBO for sales analysis
sales_df = spark.read.table("sales_fact")
optimizer.enable_cost_based_optimization(sales_df, "sales_analysis")

# Output shows:
# Table statistics for sales_analysis:
# +----------+------------------+
# |  col_name|             value|
# +----------+------------------+
# |Statistics|   1234567 rows...|
# |  numFiles|                45|
# |   numRows|           1234567|
# |sizeInBytes|        5234567890|
# +----------+------------------+

# CBO now optimizes all queries using this table
optimized_query = spark.sql("""
    SELECT customer_id, SUM(amount) as total_sales
    FROM sales_analysis
    WHERE sale_date >= '2024-01-01' AND region = 'NORTH'
    GROUP BY customer_id
    HAVING total_sales > 10000
""")
```

### When to Use CBO
- ✅ **Complex queries** with multiple joins
- ✅ **Large tables** where statistics matter
- ✅ **Analytical workloads** with aggregations
- ✅ **Data warehouse** scenarios

### CBO Configuration Options

| Configuration | Default | Purpose | When to Adjust |
|---------------|---------|---------|----------------|
| `spark.sql.cbo.enabled` | `false` | Enable CBO | Always enable for analytics |
| `spark.sql.cbo.joinReorder.enabled` | `false` | Auto join reordering | Enable for multi-table joins |
| `spark.sql.statistics.histogram.enabled` | `false` | Column histograms | Better selectivity estimation |
| `spark.sql.statistics.histogram.numBins` | `254` | Histogram detail | More bins = better accuracy |

```python
# Configure CBO for maximum optimization
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
spark.conf.set("spark.sql.statistics.histogram.numBins", "500")
```

## Method: optimize_predicates_pushdown()

### Purpose
Demonstrates predicate pushdown optimization to reduce data transfer from source systems.

### Parameters
- `server`, `database`, `username`, `password`: Database connection details
- `table_name`: Source table name

### How Predicate Pushdown Works
1. **Filter Analysis**: Spark analyzes filter conditions
2. **Pushdown Decision**: Determines which filters can be pushed to source
3. **Source Query**: Modifies source query to include filters
4. **Reduced Transfer**: Only filtered data is transferred

### Supported Pushdown Operations
- **Filter Predicates**: WHERE clauses
- **Projection Pushdown**: SELECT column lists
- **Aggregate Pushdown**: Simple aggregations (COUNT, SUM, etc.)
- **Limit Pushdown**: LIMIT clauses

**Example Usage:**
```python
# Read with automatic predicate pushdown
df = optimizer.optimize_predicates_pushdown(
    server="sql-server.company.com",
    database="sales_db",
    username="etl_user",
    password="password",
    table_name="sales_transactions"
)

# The filters below are pushed down to SQL Server:
# - status = 'active'
# - created_date >= '2024-01-01'
# SQL Server executes: 
# SELECT * FROM sales_transactions 
# WHERE status = 'active' AND created_date >= '2024-01-01'

# Only filtered results are transferred to Spark
```

### Predicate Pushdown Configuration

| Property | Value | Purpose | Impact |
|----------|-------|---------|---------|
| `pushDownPredicate` | `"true"` | Enable filter pushdown | 50-90% less data transfer |
| `pushDownAggregate` | `"true"` | Enable aggregate pushdown | 80-95% less data transfer |
| `pushDownLimit` | `"true"` | Enable limit pushdown | Faster sampling queries |

### Pushdown Compatibility

| Database | Filter Pushdown | Aggregate Pushdown | Limit Pushdown |
|----------|----------------|-------------------|----------------|
| **SQL Server** | ✅ Full support | ✅ Basic aggregates | ✅ TOP clause |
| **PostgreSQL** | ✅ Full support | ✅ Full support | ✅ LIMIT clause |
| **MySQL** | ✅ Full support | ✅ Basic aggregates | ✅ LIMIT clause |
| **Oracle** | ✅ Full support | ✅ Basic aggregates | ✅ ROWNUM |

## Method: optimize_joins_strategies()

### Purpose
Demonstrates different join strategies and optimization techniques for various scenarios.

### Parameters
- `large_df`: Large DataFrame (fact table)
- `medium_df`: Medium-sized DataFrame
- `small_df`: Small DataFrame (dimension table)

### Join Strategy Types

#### 1. Broadcast Hash Join
- **Best for**: Small dimension tables (<200MB)
- **How it works**: Small table broadcast to all nodes
- **Benefits**: No shuffle of large table required

```python
# Automatic broadcast (tables < spark.sql.autoBroadcastJoinThreshold)
df_result = large_df.join(small_df, "key")

# Manual broadcast hint
df_result = large_df.join(broadcast(small_df), "key")
```

#### 2. Sort-Merge Join
- **Best for**: Large-large table joins
- **How it works**: Both tables sorted and merged
- **Benefits**: Handles large datasets efficiently

```python
# Spark automatically chooses for large tables
df_result = large_df.join(medium_df, "key")

# Force sort-merge join
df_result = large_df.hint("MERGE").join(medium_df, "key")
```

#### 3. Shuffle Hash Join
- **Best for**: Medium-large joins with memory available
- **How it works**: Smaller table built into hash table
- **Benefits**: Faster than sort-merge when hash table fits in memory

```python
# Hint for shuffle hash join
df_result = large_df.hint("SHUFFLE_HASH").join(medium_df, "key")
```

#### 4. Bucketed Join
- **Best for**: Pre-partitioned tables on join keys
- **How it works**: No shuffle needed, direct partition-to-partition joins
- **Benefits**: Fastest possible joins

```python
# Both tables must be bucketed on join key
# No special syntax needed - Spark detects automatically
df_result = bucketed_table1.join(bucketed_table2, "bucket_key")
```

**Example Usage:**
```python
# Create sample DataFrames
large_sales = spark.range(10000000).select(
    col("id").alias("sale_id"),
    (col("id") % 1000).alias("customer_id")
)

medium_orders = spark.range(1000000).select(
    col("id").alias("order_id"), 
    (col("id") % 1000).alias("customer_id")
)

small_customers = spark.range(1000).select(
    col("id").alias("customer_id"),
    concat(lit("Customer_"), col("id")).alias("name")
)

# Get optimized join strategy
result = optimizer.optimize_joins_strategies(large_sales, medium_orders, small_customers)
```

### Join Strategy Selection

| Table Sizes | Recommended Strategy | Expected Performance |
|-------------|---------------------|---------------------|
| **Large + Small** | Broadcast Hash | 5-20x faster |
| **Large + Large** | Sort-Merge | Baseline |
| **Large + Medium** | Shuffle Hash | 2-5x faster |
| **Bucketed Tables** | Bucketed Join | 10-50x faster |

### Join Optimization Best Practices

```python
# 1. Join smaller tables first
optimized_join = small_df.join(medium_df, "key").join(large_df, "key")

# 2. Filter before joining
filtered_large = large_df.filter(col("active") == True)
result = filtered_large.join(small_df, "key")

# 3. Use appropriate join types
inner_join = df1.join(df2, "key", "inner")        # Most efficient
left_join = df1.join(df2, "key", "left")          # When needed
full_join = df1.join(df2, "key", "full")          # Least efficient

# 4. Consider bucketing for frequently joined tables
df.write.bucketBy(16, "join_key").saveAsTable("bucketed_table")
```

## Method: optimize_aggregations()

### Purpose
Optimizes aggregation operations for better performance and resource utilization.

### Aggregation Optimization Techniques

#### 1. Partial Aggregations
```python
# Group by high cardinality columns first for better distribution
optimized_agg = df \
    .groupBy("customer_id", "region") \  # High cardinality first
    .agg(sum("amount").alias("total_amount"),
         count("*").alias("transaction_count"),
         avg("amount").alias("avg_amount"))
```

#### 2. Approximate Aggregations
```python
# Use approximate functions for large datasets
approx_agg = df \
    .groupBy("category") \
    .agg(
        approx_count_distinct("customer_id").alias("approx_customers"),
        expr("percentile_approx(amount, 0.5)").alias("median_amount"),
        expr("percentile_approx(amount, array(0.25, 0.75))").alias("quartiles")
    )
```

#### 3. Pre-Aggregation Strategy
```python
# Pre-aggregate at finer granularity, then roll up
daily_agg = df \
    .groupBy("date", "product_id") \
    .agg(sum("sales").alias("daily_sales"))

monthly_agg = daily_agg \
    .withColumn("month", date_trunc("month", col("date"))) \
    .groupBy("month", "product_id") \
    .agg(sum("daily_sales").alias("monthly_sales"))
```

**Example Usage:**
```python
# Sample transaction data
transactions_df = spark.createDataFrame([
    (1, "2024-01-15", "Electronics", "CUST001", 1500),
    (2, "2024-01-16", "Clothing", "CUST002", 800),
    # ... more data
], ["id", "date", "category", "customer_id", "amount"])

# Apply aggregation optimizations
optimized_agg, approx_agg = optimizer.optimize_aggregations(transactions_df)

# Results show performance improvements:
# - Partial aggregations: 30-60% faster
# - Approximate aggregations: 70-90% faster for large datasets
```

### Aggregation Function Performance

| Function Type | Exact | Approximate | Performance Gain |
|---------------|-------|-------------|------------------|
| **COUNT DISTINCT** | `countDistinct()` | `approx_count_distinct()` | 5-20x faster |
| **PERCENTILES** | `expr("percentile()")` | `expr("percentile_approx()")` | 10-50x faster |
| **QUANTILES** | `quantile()` | `approx_quantile()` | 8-30x faster |

## Method: optimize_window_functions()

### Purpose
Optimizes window function operations for analytical queries.

### Window Function Optimization

#### 1. Efficient Window Specifications
```python
from pyspark.sql.window import Window

# Optimize window partitioning
efficient_window = Window \
    .partitionBy("category") \
    .orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Apply window functions
result = df \
    .withColumn("running_total", sum("amount").over(efficient_window)) \
    .withColumn("row_number", row_number().over(efficient_window)) \
    .withColumn("rank", dense_rank().over(efficient_window))
```

#### 2. Limited Window Frames
```python
# Use bounded windows for better performance
bounded_window = Window \
    .partitionBy("customer_id") \
    .orderBy("purchase_date") \
    .rowsBetween(-6, 0)  # Last 7 transactions only

rolling_avg = df.withColumn(
    "rolling_avg_7_days", 
    avg("amount").over(bounded_window)
)
```

**Example Usage:**
```python
# Sales data with window functions
sales_df = spark.createDataFrame([
    ("CUST001", "2024-01-01", 1000),
    ("CUST001", "2024-01-02", 1500),
    ("CUST002", "2024-01-01", 800),
], ["customer_id", "date", "amount"])

windowed_df = optimizer.optimize_window_functions(sales_df)
```

### Window Function Best Practices

| Technique | Description | Performance Impact |
|-----------|-------------|-------------------|
| **Partition Wisely** | Use columns with good cardinality | 2-5x faster |
| **Limit Frames** | Use bounded windows when possible | 3-10x faster |
| **Cache Results** | Cache windowed DataFrames | Avoid recomputation |
| **Combine Operations** | Multiple window functions in single pass | 50% faster |

## Method: optimize_column_operations()

### Purpose
Optimizes column-level operations and projections for better performance.

### Column Optimization Techniques

#### 1. Early Projection
```python
# Select only needed columns early in the pipeline
optimized_df = df.select("id", "customer_id", "amount", "date") \
    .filter(col("amount") > 100) \
    .withColumn("amount_category", 
               when(col("amount") > 1000, "high")
               .when(col("amount") > 500, "medium")
               .otherwise("low"))
```

#### 2. Efficient Column Operations
```python
# Use built-in functions instead of UDFs
optimized_df = df \
    .withColumn("amount_squared", col("amount") * col("amount")) \
    .withColumn("name_upper", upper(col("name"))) \
    .withColumn("year", year(col("date"))) \
    .withColumn("is_premium", col("amount") > 1000)
```

#### 3. Batch Column Operations
```python
# Apply multiple column operations together
from pyspark.sql.functions import *

batch_operations = df.select(
    col("id"),
    col("name"),
    upper(col("name")).alias("name_upper"),
    col("amount"),
    (col("amount") * 1.1).alias("amount_with_tax"),
    when(col("amount") > 1000, "premium").otherwise("standard").alias("tier"),
    year(col("date")).alias("year"),
    month(col("date")).alias("month")
)
```

## Method: optimize_sql_queries()

### Purpose
Demonstrates SQL query optimization patterns for better performance.

### SQL Optimization Patterns

#### 1. Efficient EXISTS vs IN
```python
# Use EXISTS instead of IN for better performance
optimized_exists = spark.sql("""
    SELECT * FROM large_table t1
    WHERE EXISTS (
        SELECT 1 FROM lookup_table t2 
        WHERE t2.key = t1.key AND t2.status = 'active'
    )
""")

# Less efficient IN clause
# SELECT * FROM large_table WHERE key IN (SELECT key FROM lookup_table)
```

#### 2. Optimized CASE WHEN
```python
# Efficient CASE WHEN structure
case_when_optimized = spark.sql("""
    SELECT *,
        CASE 
            WHEN amount > 10000 THEN 'enterprise'
            WHEN amount > 1000 THEN 'business'
            WHEN amount > 100 THEN 'standard'
            ELSE 'basic'
        END as customer_tier
    FROM transactions
""")
```

#### 3. Common Table Expressions (CTEs)
```python
# Use CTEs for complex queries
cte_optimized = spark.sql("""
    WITH ranked_customers AS (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_sales DESC) as rn
        FROM customer_summary
    ),
    top_customers AS (
        SELECT * FROM ranked_customers WHERE rn <= 10
    )
    SELECT region, COUNT(*) as top_customer_count
    FROM top_customers
    GROUP BY region
""")
```

## Method: cache_strategy_optimization()

### Purpose
Implements optimal caching strategies for query performance.

### Caching Best Practices

#### 1. Strategic Caching Points
```python
# Cache expensive intermediate results
expensive_df = df \
    .join(dimension_table, "key") \
    .withColumn("complex_calculation", 
               when(col("type") == "A", col("value") * 1.2)
               .when(col("type") == "B", col("value") * 0.8)
               .otherwise(col("value"))) \
    .filter(col("active") == True)

# Cache before multiple operations
expensive_df.cache()

# Multiple operations benefit from cache
result1 = expensive_df.groupBy("category").sum("value")
result2 = expensive_df.filter(col("region") == "NORTH").count()
result3 = expensive_df.orderBy("value").limit(100)
```

#### 2. Storage Level Selection
```python
from pyspark import StorageLevel

# Memory-only for frequently accessed small datasets
df.persist(StorageLevel.MEMORY_ONLY)

# Memory + disk for larger datasets
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Replicated for high availability
df.persist(StorageLevel.MEMORY_AND_DISK_2)
```

## Complete Usage Examples

### Example 1: Data Warehouse Query Optimization
```python
# Initialize optimizer
optimizer = SparkQueryOptimizer("DataWarehouseETL")

# Step 1: Enable CBO for fact table
fact_sales = spark.read.table("fact_sales")
optimizer.enable_cost_based_optimization(fact_sales, "fact_sales_optimized")

# Step 2: Optimize dimension joins
dim_customer = spark.read.table("dim_customer")
dim_product = spark.read.table("dim_product")

# Use broadcast joins for dimensions
enriched_sales = optimizer.optimize_joins_strategies(fact_sales, dim_customer, dim_product)

# Step 3: Optimize aggregations
monthly_summary = optimizer.optimize_aggregations(enriched_sales)

# Step 4: Apply caching for repeated access
monthly_summary.cache()

# Multiple analytical queries benefit from optimizations
```

### Example 2: Real-time Analytics Pipeline
```python
# Stream processing with query optimization
streaming_df = spark.readStream.format("kafka").load()

# Apply predicate pushdown where possible
filtered_stream = streaming_df.filter(col("event_type") == "purchase")

# Optimize window aggregations
windowed_stream = optimizer.optimize_window_functions(filtered_stream)

# Write optimized results
windowed_stream.writeStream \
    .outputMode("append") \
    .format("delta") \
    .start("/data/optimized_stream")
```

This comprehensive documentation covers all query optimization techniques with practical examples and performance considerations for analytical workloads.
