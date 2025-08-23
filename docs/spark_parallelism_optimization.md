# Spark Parallelism Optimization Documentation

## Overview
The `spark_parallelism_optimization.py` module demonstrates how to ensure proper parallelism and optimize Spark performance through configuration tuning, broadcast joins, and shuffle optimization.

## Class: SparkParallelismOptimizer

### Initialization

```python
optimizer = SparkParallelismOptimizer(app_name="SparkParallelism")
```

#### Spark Configuration Options

| Configuration | Value | Purpose | Performance Impact |
|---------------|-------|---------|-------------------|
| `spark.sql.adaptive.enabled` | `"true"` | Enable Adaptive Query Execution (AQE) | 20-50% query performance improvement |
| `spark.sql.adaptive.coalescePartitions.enabled` | `"true"` | Automatically merge small partitions | Reduces task overhead, fewer small files |
| `spark.sql.adaptive.coalescePartitions.minPartitionNum` | `"1"` | Minimum partitions after coalescing | Prevents over-coalescing |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `"128MB"` | Target partition size for AQE | Optimal balance of parallelism vs overhead |
| `spark.sql.adaptive.skewJoin.enabled` | `"true"` | Handle skewed joins automatically | 2-10x faster joins with skewed data |
| `spark.default.parallelism` | `"200"` | Default parallelism for RDD operations | Should be 2-3x number of CPU cores |
| `spark.sql.shuffle.partitions` | `"200"` | Partitions for shuffle operations | Critical for join/aggregation performance |

### Configuration Guidelines

#### Setting Default Parallelism
```python
# Rule of thumb: 2-4x number of CPU cores
cluster_cores = 64  # Total cores in cluster
recommended_parallelism = cluster_cores * 3  # 192

spark.conf.set("spark.default.parallelism", str(recommended_parallelism))
spark.conf.set("spark.sql.shuffle.partitions", str(recommended_parallelism))
```

#### Adaptive Query Execution Benefits
- **Dynamic Coalescing**: Reduces small partitions automatically
- **Dynamic Partition Pruning**: Skips unnecessary partitions
- **Dynamic Join Selection**: Chooses optimal join strategy at runtime
- **Skew Handling**: Splits large partitions automatically

**Example Usage:**
```python
# Initialize with custom parallelism
optimizer = SparkParallelismOptimizer("ProductionETL")

# Access underlying configurations
print(f"Default Parallelism: {optimizer.spark.sparkContext.defaultParallelism}")
print(f"Shuffle Partitions: {optimizer.spark.conf.get('spark.sql.shuffle.partitions')}")
```

## Method: check_current_parallelism()

### Purpose
Analyzes current parallelism settings and DataFrame partition distribution to identify optimization opportunities.

### Parameters
- `df`: DataFrame to analyze

### Returns
- List of partition sizes (number of records per partition)

### Key Metrics Analyzed
1. **Default Parallelism**: Base parallelism for RDD operations
2. **DataFrame Partitions**: Current number of partitions in the DataFrame
3. **Shuffle Partitions**: Partitions used for shuffle operations
4. **Partition Distribution**: Records per partition (identifies skew)

**Example Usage:**
```python
# Analyze partition distribution
sales_df = spark.read.parquet("/data/sales")
partition_info = optimizer.check_current_parallelism(sales_df)

# Output:
# Default Parallelism: 200
# DataFrame Partitions: 45
# Shuffle Partitions: 200
# Records per partition: [156234, 167891, 145672, ..., 134567]

# Identify skew
avg_partition_size = sum(partition_info) / len(partition_info)
max_partition_size = max(partition_info)
skew_ratio = max_partition_size / avg_partition_size

if skew_ratio > 2:
    print(f"High skew detected! Ratio: {skew_ratio:.2f}")
    # Consider repartitioning or using different strategy
```

### Partition Analysis Interpretation

| Partition Count vs Cores | Assessment | Action |
|-------------------------|------------|---------|
| < 0.5x cores | Under-partitioned | Increase partitions |
| 0.5x - 2x cores | Good | Monitor performance |
| 2x - 4x cores | Optimal | No action needed |
| > 4x cores | Over-partitioned | Reduce partitions |

| Skew Ratio | Assessment | Action |
|------------|------------|---------|
| < 1.5 | Well balanced | No action |
| 1.5 - 2.0 | Mild skew | Monitor |
| 2.0 - 5.0 | Moderate skew | Consider repartitioning |
| > 5.0 | High skew | Urgent optimization needed |

## Method: optimize_partitioning_for_read()

### Purpose
Demonstrates optimized JDBC reading with proper partitioning for parallel data extraction.

### Parameters
- `server`: Database server
- `database`: Database name
- `username`: Database username
- `password`: Database password
- `table_name`: Table to read from
- `partition_column`: Column to partition on
- `lower_bound`: Lower bound for partitioning
- `upper_bound`: Upper bound for partitioning
- `num_partitions`: Number of parallel partitions

### Optimization Benefits
- **Parallel Extraction**: Multiple concurrent database connections
- **Balanced Load**: Even distribution of data across partitions
- **Resource Utilization**: Maximizes cluster CPU/network usage

**Example Usage:**
```python
# Read large table with optimal partitioning
df = optimizer.optimize_partitioning_for_read(
    server="sql-server.company.com",
    database="sales_db",
    username="etl_user",
    password="password",
    table_name="large_sales_table",
    partition_column="order_id",
    lower_bound=1,
    upper_bound=10000000,
    num_partitions=40  # 40 parallel database connections
)

# This creates 40 parallel queries:
# SELECT * FROM large_sales_table WHERE order_id >= 1 AND order_id < 250000
# SELECT * FROM large_sales_table WHERE order_id >= 250000 AND order_id < 500000
# ...
# SELECT * FROM large_sales_table WHERE order_id >= 9750000 AND order_id <= 10000000
```

### Performance Considerations

| Factor | Optimal Value | Impact |
|--------|---------------|---------|
| **Num Partitions** | 2-4x CPU cores | Maximize parallelism |
| **Partition Size** | 100K-1M rows | Balance memory vs overhead |
| **Database Connections** | Monitor DB connection pool | Avoid overwhelming source |
| **Network Bandwidth** | Consider available bandwidth | More partitions = more network usage |

## Method: demonstrate_broadcast_join()

### Purpose
Shows performance comparison between regular joins and broadcast joins for small table optimization.

### Parameters
- `large_df`: Large DataFrame (typically fact table)
- `small_df`: Small DataFrame (typically dimension table)

### How Broadcast Joins Work
1. **Small Table Broadcasting**: Small table is sent to all worker nodes
2. **Local Joins**: Joins performed locally without shuffling large table
3. **No Network Shuffle**: Eliminates expensive shuffle operations

### When to Use Broadcast Joins
- ✅ **Small dimension tables** (<200MB by default)
- ✅ **Star schema joins** (fact table with dimensions)
- ✅ **Lookup table joins**
- ✅ **Reference data joins**

### When NOT to Use Broadcast Joins
- ❌ **Large tables** (>200MB, causes driver memory issues)
- ❌ **Similar-sized tables** (no clear small/large distinction)
- ❌ **Memory-constrained clusters**

**Example Usage:**
```python
# Create sample data
large_sales_df = spark.range(10000000).select(
    col("id").alias("sale_id"),
    (col("id") % 1000).alias("customer_id"),
    (rand() * 1000).alias("amount")
)

small_customer_df = spark.range(1000).select(
    col("id").alias("customer_id"),
    concat(lit("Customer_"), col("id")).alias("customer_name")
)

# Compare join performance
result_df = optimizer.demonstrate_broadcast_join(large_sales_df, small_customer_df)

# Output:
# Regular join time: 45.23s, Count: 10000000
# Broadcast join time: 12.67s, Count: 10000000
# Performance improvement: 3.6x faster!
```

### Broadcast Join Configuration

| Configuration | Default | Purpose | When to Adjust |
|---------------|---------|---------|----------------|
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | Auto-broadcast threshold | Increase for more memory, decrease for safety |
| `spark.sql.broadcastTimeout` | `300s` | Broadcast timeout | Increase for slow networks |
| `spark.driver.maxResultSize` | `1GB` | Max result size to driver | Must be larger than broadcasted table |

```python
# Adjust broadcast settings
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200MB")  # Larger auto-broadcast
spark.conf.set("spark.sql.broadcastTimeout", "600")             # Longer timeout
```

### Manual Broadcast Hints
```python
from pyspark.sql.functions import broadcast

# Force broadcast even if table is larger than threshold
df_result = large_df.join(broadcast(medium_df), "key")

# Disable broadcast for a specific join
df_result = large_df.hint("NO_BROADCAST_HASH").join(medium_df, "key")
```

## Method: optimize_shuffle_operations()

### Purpose
Demonstrates techniques to minimize expensive shuffle operations that move data across the network.

### Shuffle-Causing Operations
- **Joins**: Except broadcast joins
- **GroupBy/Aggregations**: Data regrouping by keys
- **Window Functions**: Partitioning and ordering
- **Distinct**: Removing duplicates
- **OrderBy**: Global sorting
- **Repartition**: Explicit reshuffling

### Optimization Techniques

#### 1. Caching Strategy
```python
# Cache DataFrames used multiple times to avoid recomputation
df.cache()  # or df.persist()

# Use cached DataFrame for multiple operations
result1 = df.groupBy("category").sum("amount")
result2 = df.filter(col("status") == "active")
result3 = df.join(other_df, "key")
```

#### 2. Operation Combining
```python
# Bad: Multiple shuffles
df.groupBy("category").sum("amount") \
  .filter(col("sum(amount)") > 1000) \
  .orderBy("sum(amount)")

# Good: Combined operations minimize shuffles
df.filter(col("amount") > 0) \  # Push down filters
  .groupBy("category") \
  .sum("amount") \
  .filter(col("sum(amount)") > 1000) \
  .orderBy("sum(amount)")
```

#### 3. Reduce vs GroupBy
```python
# Less efficient: groupBy creates more shuffle
df.groupBy("key").agg(sum("value"))

# More efficient: reduceByKey (when possible)
df.rdd.map(lambda x: (x.key, x.value)) \
      .reduceByKey(lambda a, b: a + b)
```

**Example Usage:**
```python
# Example DataFrame with optimization opportunities
sales_df = spark.createDataFrame([
    (1, "Electronics", 1500, "active"),
    (2, "Clothing", 800, "active"),
    (3, "Electronics", 1200, "inactive"),
    # ... more data
], ["id", "category", "amount", "status"])

# Apply shuffle optimizations
optimized_df = optimizer.optimize_shuffle_operations(sales_df)

# This applies several optimizations:
# 1. Filters before aggregation (reduces shuffle data)
# 2. Combines select with groupBy
# 3. Single orderBy at the end
```

### Shuffle Optimization Best Practices

| Technique | Description | Performance Gain |
|-----------|-------------|------------------|
| **Filter Early** | Apply filters before shuffles | 2-5x faster |
| **Combine Operations** | Chain operations to minimize shuffles | 1.5-3x faster |
| **Appropriate Partitioning** | Pre-partition for joins | 3-10x faster |
| **Broadcast Small Tables** | Avoid shuffling large tables | 2-8x faster |
| **Cache Intermediate Results** | Avoid recomputation | 2-4x faster |

## Method: memory_optimization_techniques()

### Purpose
Demonstrates memory optimization techniques for efficient data processing.

### Storage Levels

| Storage Level | Memory | Disk | Serialized | Replicated | Use Case |
|---------------|--------|------|------------|------------|----------|
| `MEMORY_ONLY` | ✅ | ❌ | ❌ | ❌ | Fast access, enough memory |
| `MEMORY_AND_DISK` | ✅ | ✅ | ❌ | ❌ | Balanced (default) |
| `MEMORY_ONLY_SER` | ✅ | ❌ | ✅ | ❌ | Memory efficient |
| `MEMORY_AND_DISK_SER` | ✅ | ✅ | ✅ | ❌ | Memory efficient + spill |
| `DISK_ONLY` | ❌ | ✅ | ❌ | ❌ | Memory constrained |
| `MEMORY_AND_DISK_2` | ✅ | ✅ | ❌ | ✅ | High availability |

**Example Usage:**
```python
from pyspark import StorageLevel

# Memory-rich environment
df.persist(StorageLevel.MEMORY_ONLY)

# Production environment (balanced)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Memory-constrained environment
df.persist(StorageLevel.DISK_ONLY)

# High-availability requirement
df.persist(StorageLevel.MEMORY_AND_DISK_2)

# Clean up when done
df.unpersist()
```

### Memory Management Best Practices
```python
def memory_optimization_techniques(self, df):
    # 1. Choose appropriate storage level
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    # 2. Use columnar storage formats
    df.write.parquet("/data/optimized")  # Parquet is columnar
    
    # 3. Project only needed columns
    df.select("id", "amount", "date")  # Don't select all columns
    
    # 4. Filter early to reduce memory usage
    df.filter(col("date") >= "2024-01-01")
    
    # 5. Use appropriate data types
    df.withColumn("amount", col("amount").cast("decimal(10,2)"))  # vs double
    
    return df
```

## Method: parallel_write_optimization()

### Purpose
Optimizes parallel write operations for maximum throughput while controlling file sizes.

### Write Optimization Strategies

#### 1. Optimal Partitioning for Writes
```python
# Calculate optimal partitions for target file size
target_file_size_mb = 128  # 128MB per file
estimated_size_mb = df.count() * len(df.columns) * 8 / (1024 * 1024)
optimal_partitions = max(1, int(estimated_size_mb / target_file_size_mb))

# Repartition before writing
optimized_df = df.repartition(optimal_partitions)
```

#### 2. Format-Specific Optimizations
```python
# Parquet optimization
df.repartition(16) \
  .write \
  .mode("overwrite") \
  .option("compression", "snappy") \
  .option("parquet.block.size", "134217728") \  # 128MB blocks
  .parquet("/data/optimized_parquet")

# Delta Lake optimization
df.repartition(12) \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("mergeSchema", "true") \
  .save("/data/optimized_delta")
```

**Example Usage:**
```python
# Large DataFrame that needs optimized writing
large_df = spark.range(10000000).select(
    col("id"),
    (rand() * 1000).alias("value1"),
    (rand() * 100).alias("value2")
)

optimizer.parallel_write_optimization(
    df=large_df,
    output_path="/data/optimized_output",
    file_format="parquet"
)

# Results in:
# - Optimal number of output files
# - Target file sizes (~128MB each)
# - Maximum write parallelism
# - Proper compression settings
```

## Complete Usage Examples

### Example 1: ETL Pipeline Optimization
```python
# Initialize optimizer
optimizer = SparkParallelismOptimizer("ETLPipeline")

# Step 1: Read with optimal partitioning
source_df = optimizer.optimize_partitioning_for_read(
    server="source-db.company.com",
    database="sales",
    username="etl_user",
    password="password",
    table_name="sales_transactions",
    partition_column="transaction_id",
    lower_bound=1,
    upper_bound=5000000,
    num_partitions=50
)

# Step 2: Check and optimize parallelism
partition_info = optimizer.check_current_parallelism(source_df)

# Step 3: Join with dimension data using broadcast
dim_customer = spark.read.parquet("/data/dimensions/customers")  # Small table
enriched_df = optimizer.demonstrate_broadcast_join(source_df, dim_customer)

# Step 4: Apply transformations with shuffle optimization
final_df = optimizer.optimize_shuffle_operations(enriched_df)

# Step 5: Write with optimal parallelism
optimizer.parallel_write_optimization(final_df, "/data/output", "parquet")
```

### Example 2: Performance Monitoring and Tuning
```python
# Analyze existing DataFrame performance
sales_df = spark.read.table("sales_fact")

# Check current parallelism
partition_info = optimizer.check_current_parallelism(sales_df)
avg_size = sum(partition_info) / len(partition_info)
max_size = max(partition_info)
skew_ratio = max_size / avg_size

print(f"Average partition size: {avg_size:.0f}")
print(f"Max partition size: {max_size}")
print(f"Skew ratio: {skew_ratio:.2f}")

# Optimize based on analysis
if skew_ratio > 2:
    print("High skew detected - repartitioning...")
    sales_df = sales_df.repartition(col("region"), col("product_category"))
    
if len(partition_info) > 1000:
    print("Too many partitions - coalescing...")
    sales_df = sales_df.coalesce(200)
    
if avg_size < 10000:
    print("Small partitions - reducing partition count...")
    sales_df = sales_df.coalesce(len(partition_info) // 4)
```

### Example 3: Memory-Constrained Environment
```python
# Configuration for memory-constrained cluster
optimizer.spark.conf.set("spark.sql.shuffle.partitions", "100")  # Fewer partitions
optimizer.spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

# Use memory-efficient storage
large_df = spark.read.parquet("/data/large_dataset")
large_df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Serialized + spill to disk

# Process in smaller chunks
def process_chunk(chunk_df):
    return chunk_df.groupBy("category").sum("amount")

# Process data in batches to control memory
results = []
for partition_id in range(large_df.rdd.getNumPartitions()):
    chunk = large_df.filter(spark_partition_id() == partition_id)
    result = process_chunk(chunk)
    results.append(result)

# Combine results
final_result = results[0]
for result in results[1:]:
    final_result = final_result.union(result)
```

## Performance Monitoring

### Key Metrics to Monitor
```python
# Partition distribution
def analyze_partition_health(df):
    partition_sizes = df.rdd.glom().map(len).collect()
    
    metrics = {
        "num_partitions": len(partition_sizes),
        "total_records": sum(partition_sizes),
        "avg_partition_size": sum(partition_sizes) / len(partition_sizes),
        "min_partition_size": min(partition_sizes),
        "max_partition_size": max(partition_sizes),
        "skew_ratio": max(partition_sizes) / (sum(partition_sizes) / len(partition_sizes))
    }
    
    return metrics

# Usage
metrics = analyze_partition_health(df)
print(f"Partition Health: {metrics}")
```

### Spark UI Monitoring
- **Jobs Tab**: Monitor task duration and data skew
- **Stages Tab**: Identify shuffle operations and bottlenecks  
- **Storage Tab**: Monitor cached DataFrame memory usage
- **Executors Tab**: Check CPU and memory utilization

This comprehensive documentation covers all parallelism optimization techniques with practical examples and performance monitoring strategies.
