# Spark Partitioning Strategies Documentation

## Overview
The `spark_partitioning_strategies.py` module demonstrates when and how to use different partitioning and repartitioning strategies for optimal Spark performance.

## Class: SparkPartitioningStrategies

### Initialization

```python
partitioner = SparkPartitioningStrategies(app_name="SparkPartitioning")
```

#### Spark Configuration Options

| Configuration | Value | Purpose | Performance Impact |
|---------------|-------|---------|-------------------|
| `spark.sql.adaptive.enabled` | `"true"` | Enable Adaptive Query Execution | Automatic partition optimization |
| `spark.sql.adaptive.coalescePartitions.enabled` | `"true"` | Auto-merge small partitions | Reduces small file problems |
| `spark.sql.adaptive.skewJoin.enabled` | `"true"` | Handle skewed data in joins | Better join performance |

## Method: analyze_partition_distribution()

### Purpose
Analyzes current partition distribution to identify optimization opportunities and performance issues.

### Parameters
- `df`: DataFrame to analyze
- `description`: Description for logging output

### Returns
- List of partition sizes (records per partition)

### Key Metrics Analyzed
1. **Number of Partitions**: Total partition count
2. **Records per Partition**: Distribution across partitions
3. **Min/Max Partition Size**: Identifies skew
4. **Average Partition Size**: Baseline for comparison

**Example Usage:**
```python
# Analyze partition distribution
sales_df = spark.read.parquet("/data/sales")
partition_sizes = partitioner.analyze_partition_distribution(sales_df, "Sales Data")

# Output:
# === Sales Data Partition Analysis ===
# Number of partitions: 24
# Records per partition: [45632, 47891, 43210, ..., 46543]
# Total records: 1,123,456
# Min partition size: 41,203
# Max partition size: 52,819
# Average partition size: 46,810.67
```

### Partition Health Assessment

| Metric | Good Range | Warning Signs | Action Required |
|--------|------------|---------------|-----------------|
| **Partition Count** | 2-4x CPU cores | >10x cores or <0.5x cores | Repartition |
| **Skew Ratio** | <2.0 | 2.0-5.0 | Monitor/optimize |
| **Skew Ratio** | - | >5.0 | Immediate action |
| **Partition Size** | 100MB-1GB | <10MB or >2GB | Resize partitions |

```python
def assess_partition_health(partition_sizes):
    if not partition_sizes:
        return "No data"
    
    avg_size = sum(partition_sizes) / len(partition_sizes)
    max_size = max(partition_sizes)
    min_size = min(partition_sizes)
    skew_ratio = max_size / avg_size if avg_size > 0 else 0
    
    assessment = {
        "skew_ratio": skew_ratio,
        "health": "Good" if skew_ratio < 2 else "Warning" if skew_ratio < 5 else "Critical"
    }
    
    return assessment
```

## Method: hash_partitioning_by_column()

### Purpose
Partitions DataFrame using hash-based distribution on specified column(s) for even data distribution.

### Parameters
- `df`: DataFrame to partition
- `column_name`: Column to hash partition on
- `num_partitions`: Number of partitions (default: `spark.defaultParallelism`)

### How Hash Partitioning Works
- Uses hash function on column values: `hash(column_value) % num_partitions`
- Ensures even distribution regardless of data skew
- Same values always go to same partition
- Random distribution across partitions

### When to Use Hash Partitioning
- ✅ **Even distribution needed** regardless of data values
- ✅ **Join optimization** when both tables hash partitioned on join key
- ✅ **Aggregation optimization** for groupBy operations
- ✅ **Skewed data** that needs balanced processing

### When NOT to Use Hash Partitioning
- ❌ **Range queries** (destroys data locality)
- ❌ **Sorted operations** (randomizes order)
- ❌ **Time-series analysis** (breaks temporal locality)

**Example Usage:**
```python
# Hash partition customer data for joins
customer_df = spark.read.table("customers")
hash_partitioned_df = partitioner.hash_partitioning_by_column(
    df=customer_df,
    column_name="customer_id",
    num_partitions=32
)

# Benefits:
# - Even distribution across 32 partitions
# - Joins on customer_id are optimized
# - Aggregations by customer_id avoid shuffle
```

**Example Usage - Multiple Column Hash:**
```python
# Hash partition on multiple columns for composite keys
order_items_df = spark.read.table("order_items")
multi_hash_df = order_items_df.repartition(24, col("order_id"), col("product_id"))

# Benefits:
# - Even distribution based on combination of both columns
# - Optimized for joins on either or both columns
```

### Hash Partitioning Best Practices

| Use Case | Column Choice | Partition Count | Expected Benefit |
|----------|---------------|-----------------|------------------|
| **Customer Analysis** | `customer_id` | 2-4x cores | Even customer distribution |
| **Product Analytics** | `product_id` | 2-4x cores | Balanced product processing |
| **Multi-dimensional** | Multiple columns | 3-5x cores | Composite key optimization |
| **High Cardinality** | Unique/high cardinality | 4-6x cores | Maximum distribution |

## Method: range_partitioning_by_column()

### Purpose
Partitions DataFrame by value ranges on specified column, maintaining data ordering and locality.

### Parameters
- `df`: DataFrame to partition
- `column_name`: Column to range partition on
- `num_partitions`: Number of partitions

### How Range Partitioning Works
- Divides column value range into equal-sized ranges
- Partition 1: values in range [min, range1]
- Partition 2: values in range (range1, range2]
- Maintains order within and across partitions

### When to Use Range Partitioning
- ✅ **Range queries** (date ranges, numeric ranges)
- ✅ **Ordered data processing**
- ✅ **Time-series analysis**
- ✅ **Sequential access patterns**
- ✅ **Window functions** with ordering

### When NOT to Use Range Partitioning
- ❌ **Skewed data** (uneven range distribution)
- ❌ **Random access** patterns
- ❌ **Hash-based joins**

**Example Usage - Date Range Partitioning:**
```python
# Range partition time-series data
sales_ts_df = spark.read.table("sales_timeseries")
range_partitioned_df = partitioner.range_partitioning_by_column(
    df=sales_ts_df,
    column_name="sale_date",
    num_partitions=12  # Monthly partitions
)

# Benefits:
# - Date range queries only scan relevant partitions
# - Maintains temporal order for time-series analysis
# - Window functions over date are optimized
```

**Example Usage - Numeric Range Partitioning:**
```python
# Range partition by order amount
orders_df = spark.read.table("orders")
amount_range_df = partitioner.range_partitioning_by_column(
    df=orders_df,
    column_name="order_amount",
    num_partitions=8
)

# Query optimization:
# SELECT * FROM orders WHERE order_amount BETWEEN 100 AND 500
# - Only scans partitions containing that range
```

### Range Partitioning Considerations

| Data Characteristic | Suitability | Recommendation |
|--------------------|-------------|----------------|
| **Even Distribution** | Excellent | Use range partitioning |
| **Skewed Distribution** | Poor | Consider hash partitioning or custom ranges |
| **Temporal Data** | Excellent | Partition by time periods |
| **Sequential Access** | Excellent | Maintains locality |
| **Random Access** | Poor | Hash partitioning better |

## Method: when_to_repartition()

### Purpose
Demonstrates scenarios when repartitioning is beneficial and provides decision logic.

### Scenarios for Repartitioning

#### Scenario 1: Too Many Small Partitions
```python
# Detection logic
current_partitions = df.rdd.getNumPartitions()
estimated_size_per_partition = df.count() / current_partitions

if estimated_size_per_partition < 10000:  # Threshold: 10K records
    print("Too many small partitions detected")
    # Solution: Coalesce to reduce partitions
    optimized_df = df.coalesce(current_partitions // 4)
```

**When This Happens:**
- Reading many small files
- After aggressive filtering
- Over-partitioned source data

**Solution: Coalesce**
- Reduces partition count without full shuffle
- Maintains data locality where possible
- Faster than repartition for size reduction

#### Scenario 2: Too Few Large Partitions
```python
# Detection logic
if estimated_size_per_partition > 1000000:  # Threshold: 1M records
    print("Too few large partitions detected")
    # Solution: Repartition to increase parallelism
    optimized_df = df.repartition(current_partitions * 4)
```

**When This Happens:**
- Reading large files
- After unions of small datasets
- Under-partitioned source data

**Solution: Repartition**
- Increases parallelism
- Better resource utilization
- Prevents memory issues

#### Scenario 3: Before Expensive Operations
```python
# Before joins
df1_partitioned = df1.repartition(col("join_key"))
df2_partitioned = df2.repartition(col("join_key"))
joined = df1_partitioned.join(df2_partitioned, "join_key")

# Before aggregations
df_partitioned = df.repartition(col("group_key"))
aggregated = df_partitioned.groupBy("group_key").sum("value")
```

**Example Usage:**
```python
# Analyze and repartition based on data characteristics
sales_df = spark.read.parquet("/data/sales")
partitioner.when_to_repartition(sales_df)

# Output provides recommendations:
# Scenario 1: Too many small partitions - Consider coalescing
# Scenario 2: Too few large partitions - Consider repartitioning  
# Scenario 3: Before joins - Partition by join key
```

### Decision Matrix for Repartitioning

| Current State | Problem | Solution | Method |
|---------------|---------|----------|---------|
| **Many small partitions** | Task overhead | Reduce partitions | `coalesce()` |
| **Few large partitions** | Poor parallelism | Increase partitions | `repartition()` |
| **Skewed partitions** | Uneven processing | Even distribution | `repartition(col)` |
| **Before joins** | Shuffle overhead | Co-locate join keys | `repartition(join_col)` |
| **Before aggregations** | Multiple shuffles | Pre-partition by group key | `repartition(group_col)` |

## Method: coalesce_vs_repartition()

### Purpose
Demonstrates the differences between `coalesce()` and `repartition()` operations.

### Coalesce Operation
- **No shuffle**: Combines adjacent partitions
- **Faster**: Minimal data movement
- **May cause skew**: Uneven partition sizes
- **Use for**: Reducing partition count

### Repartition Operation
- **Full shuffle**: Redistributes all data
- **Slower**: Complete data movement
- **Even distribution**: Balanced partition sizes
- **Use for**: Increasing partitions or ensuring balance

**Example Usage:**
```python
# Compare coalesce vs repartition
large_df = spark.range(1000000).repartition(100)  # Start with 100 partitions
coalesced_df, repartitioned_df = partitioner.coalesce_vs_repartition(large_df)

# Analysis shows:
# Coalesce: 100 -> 50 partitions (fast, may be uneven)
# Repartition: 100 -> 50 partitions (slower, even distribution)
```

### When to Use Each

| Operation | Use When | Benefits | Drawbacks |
|-----------|----------|----------|-----------|
| **Coalesce** | Reducing partitions, final output | Fast, no shuffle | May create skew |
| **Repartition** | Increasing partitions, before processing | Even distribution | Shuffle overhead |

**Coalesce Example:**
```python
# Reduce partitions before writing (avoid small files)
df.coalesce(10).write.parquet("/data/output")

# Good for:
# - Final output optimization
# - Reducing small files
# - Quick partition reduction
```

**Repartition Example:**
```python
# Increase partitions for processing
df.repartition(50).groupBy("category").sum("amount")

# Good for:
# - Increasing parallelism
# - Balancing skewed data
# - Before expensive operations
```

## Method: partition_by_date_for_time_series()

### Purpose
Optimizes partitioning for time-series data using date-based partitioning strategies.

### Parameters
- `df`: DataFrame with time-series data
- `date_column`: Date column to partition on

### Date Partitioning Strategy
```python
# Extract date components for partitioning
df_with_date_parts = df \
    .withColumn("year", year(col(date_column))) \
    .withColumn("month", month(col(date_column))) \
    .withColumn("day", dayofmonth(col(date_column)))

# Partition by year and month for balanced partitions
partitioned_df = df_with_date_parts.repartition(col("year"), col("month"))
```

### Benefits of Date Partitioning
- **Temporal Locality**: Related time periods stored together
- **Range Query Optimization**: Date filters scan fewer partitions
- **Incremental Processing**: Process only new time periods
- **Data Lifecycle Management**: Easy archival of old partitions

**Example Usage:**
```python
# Partition IoT sensor data by date
sensor_df = spark.read.table("iot_sensors").withColumn("timestamp", to_date(col("event_time")))
date_partitioned_df = partitioner.partition_by_date_for_time_series(sensor_df, "timestamp")

# Benefits:
# - Queries with date filters are much faster
# - Time-series analysis maintains temporal locality
# - Monthly/yearly aggregations are optimized
```

### Time-Series Partitioning Strategies

| Time Granularity | Partition Strategy | Use Case |
|------------------|-------------------|----------|
| **High Frequency** (seconds) | Hour or Day | Real-time analytics |
| **Medium Frequency** (minutes) | Day or Week | Operational monitoring |
| **Low Frequency** (hours) | Month or Quarter | Historical analysis |
| **Batch Processing** | Year + Month | Data warehousing |

## Method: optimize_partitioning_for_joins()

### Purpose
Optimizes partitioning strategy specifically for join operations to minimize shuffle overhead.

### Parameters
- `df1`: First DataFrame to join
- `df2`: Second DataFrame to join  
- `join_column`: Column to join on

### Join Optimization Strategy
```python
# Calculate optimal partitions based on data size
num_partitions = min(200, max(df1.count(), df2.count()) // 10000)

# Partition both DataFrames by join column
df1_partitioned = df1.repartition(num_partitions, col(join_column))
df2_partitioned = df2.repartition(num_partitions, col(join_column))

# Perform join (no shuffle needed)
joined_df = df1_partitioned.join(df2_partitioned, join_column)
```

### Join Performance Benefits
- **No Shuffle**: Data with same join keys already co-located
- **Parallel Processing**: Each partition processes independently
- **Memory Efficiency**: Smaller partition-wise joins
- **Reduced Network I/O**: Minimal data movement

**Example Usage:**
```python
# Optimize join between orders and customers
orders_df = spark.read.table("orders")
customers_df = spark.read.table("customers")

optimized_join_df = partitioner.optimize_partitioning_for_joins(
    df1=orders_df,
    df2=customers_df, 
    join_column="customer_id"
)

# Performance improvement:
# - 5-10x faster joins on large datasets
# - Reduced memory pressure
# - Better cluster utilization
```

### Join Partitioning Best Practices

| Join Type | Partitioning Strategy | Expected Performance |
|-----------|----------------------|---------------------|
| **Inner Join** | Both tables by join key | 3-8x faster |
| **Left Join** | Both tables by join key | 2-5x faster |
| **Broadcast Join** | Small table broadcast | 5-20x faster |
| **Bucketed Join** | Pre-bucketed tables | 10-50x faster |

## Method: partition_for_writing()

### Purpose
Optimizes partitioning before writing data to storage for better query performance and file organization.

### Parameters
- `df`: DataFrame to write
- `output_path`: Storage path
- `partition_columns`: Columns to partition by (optional)

### Writing Strategies

#### Strategy 1: Business Logic Partitioning
```python
# Partition by business dimensions
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "region") \
    .parquet(output_path)

# Creates directory structure:
# /data/sales/year=2024/month=01/region=north/part-00000.parquet
# /data/sales/year=2024/month=01/region=south/part-00000.parquet
```

#### Strategy 2: File Size Optimization
```python
# Optimize for target file sizes
num_files = max(1, df.count() // 1000000)  # ~1M records per file
df.repartition(num_files) \
    .write \
    .mode("overwrite") \
    .parquet(output_path)
```

**Example Usage:**
```python
# Write sales data with date partitioning
sales_df = spark.read.table("sales_raw")
partitioner.partition_for_writing(
    df=sales_df,
    output_path="/data/warehouse/sales",
    partition_columns=["year", "quarter"]
)

# Benefits:
# - Query optimization through partition pruning
# - Parallel processing of partitions
# - Efficient data lifecycle management
```

### File Partitioning Guidelines

| Partition Column Type | Good Examples | Avoid |
|----------------------|---------------|--------|
| **Time-based** | year, month, day | timestamp, minute |
| **Categorical** | region, status, type | customer_id, order_id |
| **Hierarchical** | country/state/city | random groupings |
| **Business Logic** | department, product_line | derived calculations |

## Complete Usage Examples

### Example 1: ETL Pipeline Partitioning
```python
# Initialize partitioner
partitioner = SparkPartitioningStrategies("ETLPipeline")

# Step 1: Analyze source data partitioning
source_df = spark.read.table("raw_sales")
partitioner.analyze_partition_distribution(source_df, "Source Data")

# Step 2: Optimize for processing
if source_df.rdd.getNumPartitions() > 100:
    # Too many small partitions
    processing_df = source_df.coalesce(32)
else:
    # Repartition for parallel processing
    processing_df = source_df.repartition(32, col("customer_id"))

# Step 3: Optimize for joins
customer_df = spark.read.table("customers")
join_optimized_df = partitioner.optimize_partitioning_for_joins(
    df1=processing_df,
    df2=customer_df,
    join_column="customer_id"
)

# Step 4: Partition for writing
partitioner.partition_for_writing(
    df=join_optimized_df,
    output_path="/data/processed/sales",
    partition_columns=["year", "month"]
)
```

### Example 2: Time-Series Analysis Optimization
```python
# Read time-series sensor data
sensor_df = spark.read.table("iot_sensors")

# Analyze current partitioning
partitioner.analyze_partition_distribution(sensor_df, "Sensor Data")

# Optimize for time-series analysis
time_partitioned_df = partitioner.partition_by_date_for_time_series(
    df=sensor_df,
    date_column="reading_timestamp"
)

# Range partition for temporal locality
range_partitioned_df = partitioner.range_partitioning_by_column(
    df=time_partitioned_df,
    column_name="reading_timestamp",
    num_partitions=24  # Hourly partitions
)

# Write with time-based partitioning
partitioner.partition_for_writing(
    df=range_partitioned_df,
    output_path="/data/timeseries/sensors",
    partition_columns=["year", "month", "day"]
)
```

### Example 3: Multi-Table Join Optimization
```python
# Multiple large tables to join
orders_df = spark.read.table("orders")      # 100M rows
customers_df = spark.read.table("customers") # 10M rows  
products_df = spark.read.table("products")   # 1M rows

# Optimize partitioning for star schema joins
orders_partitioned = partitioner.hash_partitioning_by_column(
    df=orders_df,
    column_name="customer_id",
    num_partitions=64
)

customers_partitioned = partitioner.hash_partitioning_by_column(
    df=customers_df, 
    column_name="customer_id",
    num_partitions=64
)

# Products table is small - can be broadcast
from pyspark.sql.functions import broadcast

# Perform optimized joins
customer_orders = orders_partitioned.join(customers_partitioned, "customer_id")
final_result = customer_orders.join(broadcast(products_df), "product_id")

# Performance gains:
# - Orders-Customers join: no shuffle (same partitioning)
# - Products join: broadcast (no shuffle)
# - Overall: 10-20x faster than unoptimized joins
```

## Performance Monitoring

### Partition Health Metrics
```python
def monitor_partition_health(df, description=""):
    partition_sizes = df.rdd.glom().map(len).collect()
    
    if not partition_sizes:
        return {"status": "No data"}
    
    total_records = sum(partition_sizes)
    avg_size = total_records / len(partition_sizes)
    max_size = max(partition_sizes)
    min_size = min(partition_sizes)
    skew_ratio = max_size / avg_size if avg_size > 0 else 0
    
    # Health assessment
    if skew_ratio < 1.5:
        health = "Excellent"
    elif skew_ratio < 2.5:
        health = "Good"
    elif skew_ratio < 5.0:
        health = "Warning"
    else:
        health = "Critical"
    
    return {
        "description": description,
        "partitions": len(partition_sizes),
        "total_records": total_records,
        "avg_partition_size": avg_size,
        "skew_ratio": skew_ratio,
        "health": health
    }

# Usage
health_metrics = monitor_partition_health(df, "Sales Data")
print(f"Partition Health: {health_metrics}")
```

This comprehensive documentation covers all partitioning strategies with detailed examples and performance optimization techniques.
