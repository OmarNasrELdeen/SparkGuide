# Spark Advanced Writing Strategies Documentation

## Overview
The `spark_advanced_writing_strategies.py` module provides optimized data writing patterns for different storage scenarios, implementing data engineering best practices for bucketed tables, partitioned data, and various file formats.

## Class: SparkAdvancedWriter

### Initialization

```python
writer = SparkAdvancedWriter(app_name="SparkAdvancedWriter")
```

#### Spark Configuration Options

| Configuration | Value | Purpose | Performance Impact |
|---------------|-------|---------|-------------------|
| `spark.sql.adaptive.enabled` | `true` | Enable Adaptive Query Execution | Better query optimization |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Auto-merge small partitions | Fewer small files |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Handle skewed joins automatically | Faster joins with skewed data |
| `spark.sql.sources.bucketing.enabled` | `true` | Enable bucketing support | Join optimization |
| `spark.sql.sources.bucketing.autoBucketedScan.enabled` | `true` | Auto-detect bucketed scans | Faster bucketed table reads |
| `spark.sql.hive.metastorePartitionPruning` | `true` | Enable partition pruning | Faster queries on partitioned tables |
| `spark.serializer` | `KryoSerializer` | Fast serialization | Better performance |

**Example Usage:**
```python
# Initialize with custom configuration
writer = SparkAdvancedWriter("ProductionETL")

# Access underlying Spark session
spark = writer.spark
```

## Method: write_bucketed_table()

### Purpose
Creates bucketed tables for optimal join performance by co-locating data with the same bucket key values.

### Parameters
- `df`: DataFrame to write
- `output_path`: Storage path for the table
- `table_name`: Name of the table to create
- `bucket_columns`: List of columns to bucket on (usually join keys)
- `num_buckets`: Number of buckets to create
- `sort_columns`: Optional columns to sort within each bucket
- `file_format`: Storage format ("parquet", "orc", "delta")

### How Bucketing Works
Bucketing distributes data across a fixed number of files based on hash values of bucket columns:
- Each row is assigned to a bucket using: `hash(bucket_columns) % num_buckets`
- Rows with the same bucket key values are stored in the same files
- Enables map-side joins when joining on bucket columns

### When to Use Bucketing
- ✅ **Frequent joins** on specific columns
- ✅ **Large dimension tables** that are joined repeatedly
- ✅ **Star schema fact tables** joined with dimensions
- ✅ **Stable data** that doesn't change frequently

### When NOT to Use Bucketing
- ❌ **Small tables** (<1M rows)
- ❌ **Frequently changing data** (bucketing adds write overhead)
- ❌ **Ad-hoc queries** without consistent join patterns
- ❌ **Streaming data** (bucketing requires static partitioning)

**Example Usage - Customer Dimension Table:**
```python
# Create bucketed customer table for frequent joins
writer.write_bucketed_table(
    df=customer_df,
    output_path="/data/warehouse/customers",
    table_name="customers_bucketed",
    bucket_columns=["customer_id"],        # Primary join key
    num_buckets=16,                        # 16 files for distribution
    sort_columns=["customer_id"],          # Sort within buckets
    file_format="parquet"
)

# Benefits:
# - Joins on customer_id avoid shuffle
# - Data co-location improves cache utilization
# - Consistent file sizes improve query planning
```

**Example Usage - Product Catalog:**
```python
# Create bucketed product table with multiple join keys
writer.write_bucketed_table(
    df=product_df,
    output_path="/data/warehouse/products",
    table_name="products_bucketed",
    bucket_columns=["product_id", "category_id"],  # Multiple bucket keys
    num_buckets=32,                                # More buckets for larger table
    sort_columns=["product_id", "created_date"],   # Sort for range queries
    file_format="orc"                              # ORC for compression
)
```

### Bucketing Best Practices

#### Choosing Number of Buckets
| Data Size | Recommended Buckets | File Size per Bucket |
|-----------|-------------------|---------------------|
| < 1M rows | 4-8 | ~125K-250K rows |
| 1M-10M rows | 8-16 | ~625K-1.25M rows |
| 10M-100M rows | 16-32 | ~3.1M-6.25M rows |
| > 100M rows | 32-64 | ~1.5M-3.1M rows |

#### Bucket Column Selection
```python
# Good bucket columns (high cardinality, frequently joined)
bucket_columns=["customer_id"]           # Primary key
bucket_columns=["order_id", "customer_id"]  # Foreign key relationship

# Avoid (low cardinality, causes skew)
bucket_columns=["status"]                # Only few values
bucket_columns=["country"]               # Geographic skew
```

### File Format Optimizations

#### Parquet with Bucketing
```python
writer.write_bucketed_table(
    df=sales_df,
    table_name="sales_bucketed",
    bucket_columns=["customer_id"],
    num_buckets=20,
    file_format="parquet"
)
# Automatic optimizations:
# - compression: "snappy" (fast compression/decompression)
# - Block size optimized for bucketed reads
```

#### ORC with Bucketing
```python
writer.write_bucketed_table(
    df=transactions_df,
    table_name="transactions_bucketed", 
    bucket_columns=["account_id"],
    num_buckets=24,
    file_format="orc"
)
# Automatic optimizations:
# - compression: "zlib" (better compression ratio)
# - Stripe size optimized for bucketed access patterns
```

#### Delta Lake with Bucketing
```python
writer.write_bucketed_table(
    df=events_df,
    table_name="events_bucketed",
    bucket_columns=["user_id"],
    num_buckets=16,
    file_format="delta"
)
# Benefits:
# - ACID transactions with bucketing
# - Time travel on bucketed data
# - Schema evolution support
```

## Method: write_partitioned_table()

### Purpose
Creates partitioned tables for query optimization through partition pruning and parallel processing.

### Parameters
- `df`: DataFrame to write
- `output_path`: Storage path for the table
- `table_name`: Name of the table to create
- `partition_columns`: List of columns to partition on
- `file_format`: Storage format ("parquet", "orc", "delta")
- `dynamic_partitioning`: Enable dynamic partition creation

### Dynamic Partitioning Configuration

| Configuration | Value | Purpose | Impact |
|---------------|-------|---------|---------|
| `spark.sql.sources.partitionOverwriteMode` | `"dynamic"` | Only overwrite matching partitions | Safer incremental updates |
| `hive.exec.dynamic.partition` | `"true"` | Enable dynamic partitioning | Auto-create partitions |
| `hive.exec.dynamic.partition.mode` | `"nonstrict"` | Allow all dynamic partitions | Flexible partition creation |
| `hive.exec.max.dynamic.partitions` | `"10000"` | Maximum partitions allowed | Prevent runaway partition creation |

### When to Use Partitioning
- ✅ **Time-series data** (partition by date/time)
- ✅ **Geographical data** (partition by region/country)
- ✅ **Categorical data** with limited values
- ✅ **Range queries** on partition columns

### When NOT to Use Partitioning
- ❌ **High cardinality columns** (creates too many partitions)
- ❌ **Uniformly accessed data** (no query benefit)
- ❌ **Small datasets** (partition overhead exceeds benefits)

**Example Usage - Time-Series Partitioning:**
```python
# Partition sales data by year and month
writer.write_partitioned_table(
    df=sales_df,
    output_path="/data/warehouse/sales_partitioned",
    table_name="sales_by_month",
    partition_columns=["year", "month"],    # Hierarchical partitioning
    file_format="parquet",
    dynamic_partitioning=True
)

# Creates directory structure:
# /data/warehouse/sales_partitioned/year=2024/month=01/
# /data/warehouse/sales_partitioned/year=2024/month=02/
# ...

# Query benefits:
# SELECT * FROM sales_by_month WHERE year=2024 AND month=1
# - Only reads 1 partition instead of all data
# - Massive performance improvement for date range queries
```

**Example Usage - Regional Partitioning:**
```python
# Partition customer data by region
writer.write_partitioned_table(
    df=customer_df,
    output_path="/data/warehouse/customers_regional",
    table_name="customers_by_region",
    partition_columns=["region", "country"],  # Geographic hierarchy
    file_format="delta",
    dynamic_partitioning=True
)

# Benefits:
# - Regional queries only read relevant partitions
# - Parallel processing across regions
# - Data locality for geo-specific analysis
```

### Partitioning Best Practices

#### Partition Column Selection
```python
# Good partition columns (low-medium cardinality, frequently filtered)
partition_columns=["year", "month"]          # Time-based
partition_columns=["region"]                 # Geographic
partition_columns=["product_category"]       # Business categorization

# Avoid (high cardinality, creates too many partitions)
partition_columns=["customer_id"]            # Unique values
partition_columns=["transaction_id"]         # Unique identifiers
partition_columns=["timestamp"]              # High granularity
```

#### Optimal Partition Sizes
| Partition Size | Query Performance | Write Performance | Recommendation |
|---------------|------------------|-------------------|----------------|
| < 1MB | Poor (metadata overhead) | Fast | Combine partitions |
| 1MB-128MB | Good | Good | Optimal range |
| 128MB-1GB | Excellent | Good | Ideal for analytics |
| > 1GB | Good (but slower scans) | Slower | Consider sub-partitioning |

### File Format Specific Optimizations

#### Parquet Partitioning
```python
writer.write_partitioned_table(
    df=events_df,
    table_name="events_partitioned",
    partition_columns=["event_date"],
    file_format="parquet"
)
# Automatic optimizations:
# - compression: "snappy"
# - parquet.block.size: "134217728" (128MB)
# - Columnar compression within partitions
```

#### ORC Partitioning
```python
writer.write_partitioned_table(
    df=logs_df,
    table_name="logs_partitioned",
    partition_columns=["log_date", "log_level"],
    file_format="orc"
)
# Automatic optimizations:
# - compression: "zlib"
# - orc.stripe.size: "67108864" (64MB)
# - Better compression for text data
```

#### Delta Lake Partitioning
```python
writer.write_partitioned_table(
    df=transactions_df,
    table_name="transactions_partitioned",
    partition_columns=["transaction_date"],
    file_format="delta"
)
# Automatic optimizations:
# - mergeSchema: "true" (schema evolution)
# - ACID transactions per partition
# - Time travel across partitions
```

## Method: write_bucketed_and_partitioned_table()

### Purpose
Combines partitioning and bucketing for maximum optimization - partition pruning AND join optimization.

### Parameters
- `df`: DataFrame to write
- `output_path`: Storage path
- `table_name`: Table name
- `partition_columns`: Columns to partition on
- `bucket_columns`: Columns to bucket on
- `num_buckets`: Number of buckets per partition
- `sort_columns`: Optional sort columns
- `file_format`: Storage format

### When to Use Combined Strategy
- ✅ **Large fact tables** with time dimensions and frequent joins
- ✅ **Multi-dimensional queries** (time + joins)
- ✅ **Star schema implementations**
- ✅ **High-performance analytics** requirements

**Example Usage - Sales Fact Table:**
```python
# Optimize sales fact table for both time queries and customer joins
writer.write_bucketed_and_partitioned_table(
    df=sales_fact_df,
    output_path="/data/warehouse/sales_fact_optimized",
    table_name="sales_fact_combined",
    partition_columns=["year", "quarter"],     # Time-based partitioning
    bucket_columns=["customer_id"],            # Join optimization
    num_buckets=32,                            # Good distribution
    sort_columns=["customer_id", "sale_date"], # Range query optimization
    file_format="parquet"
)

# Directory structure:
# /sales_fact_optimized/year=2024/quarter=1/bucket_00000.parquet
# /sales_fact_optimized/year=2024/quarter=1/bucket_00001.parquet
# ...
# /sales_fact_optimized/year=2024/quarter=2/bucket_00000.parquet

# Query benefits:
# SELECT * FROM sales_fact_combined f 
# JOIN customers_bucketed c ON f.customer_id = c.customer_id
# WHERE f.year = 2024 AND f.quarter = 1
# - Partition pruning: only reads Q1 2024 data
# - Bucket pruning: no shuffle needed for join
# - Sort optimization: fast range scans within buckets
```

### Combined Strategy Benefits
1. **Partition Pruning**: Skip irrelevant time periods
2. **Bucket Join Optimization**: No shuffle for joins on bucket keys
3. **Data Locality**: Related data stored together
4. **Parallel Processing**: Each partition processes independently

## Method: write_for_initial_load_optimization()

### Purpose
Optimizes writing for initial bulk data loads without bucketing/partitioning overhead.

### Parameters
- `df`: DataFrame to write
- `output_path`: Storage path
- `file_format`: Storage format
- `target_file_size_mb`: Target size per file (default: 128MB)
- `compression`: Compression codec

### When to Use Initial Load Optimization
- ✅ **First-time data loads** into data lake/warehouse
- ✅ **Staging tables** for ETL processing
- ✅ **Large bulk imports** where write speed is priority
- ✅ **Testing/development** environments

### File Size Optimization Logic
```python
# Automatic calculation
estimated_size_mb = df.count() * len(df.columns) * 8 / (1024 * 1024)  # Rough estimate
num_files = max(1, int(estimated_size_mb / target_file_size_mb))

# Example: 10M rows, 20 columns
# Estimated size: ~1.5GB
# Target file size: 128MB
# Number of files: 12 files (~128MB each)
```

**Example Usage:**
```python
# Fast initial load of customer data
writer.write_for_initial_load_optimization(
    df=large_customer_df,
    output_path="/data/staging/customers_initial",
    file_format="parquet",
    target_file_size_mb=256,      # Larger files for fewer objects
    compression="snappy"          # Fast compression
)

# Benefits:
# - Maximum write throughput (no bucketing/partitioning overhead)
# - Optimal file sizes for subsequent processing
# - Minimal metadata overhead
```

### Compression Options

| Codec | Compression Ratio | Speed | CPU Usage | Best For |
|-------|------------------|-------|-----------|----------|
| `"none"` | 1.0x | Fastest | Minimal | Network is fast, storage is cheap |
| `"snappy"` | 2-3x | Fast | Low | Balanced performance (default) |
| `"gzip"` | 3-4x | Medium | Medium | Storage cost optimization |
| `"lz4"` | 2x | Very Fast | Low | CPU-constrained environments |
| `"zstd"` | 3-4x | Medium-Fast | Medium | Best compression with good speed |

## Method: write_with_z_ordering()

### Purpose
Implements Z-ordering (multi-dimensional clustering) for Delta Lake tables to optimize multi-column queries.

### Parameters
- `df`: DataFrame to write
- `output_path`: Storage path
- `z_order_columns`: Columns to Z-order on
- `file_format`: Must be "delta"

### How Z-Ordering Works
Z-ordering co-locates related data in the same files based on multiple dimensions:
- Maps multi-dimensional data to one dimension using Z-order curve
- Files contain ranges of values across multiple columns
- Enables efficient range queries across multiple dimensions

### When to Use Z-Ordering
- ✅ **Multi-dimensional range queries**
- ✅ **Analytics with multiple filter columns**
- ✅ **Delta Lake tables**
- ✅ **OLAP workloads**

**Example Usage:**
```python
# Z-order customer data for multi-dimensional queries
writer.write_with_z_ordering(
    df=customer_analytics_df,
    output_path="/data/delta/customers_zorder",
    z_order_columns=["age_group", "income_bracket", "region"],
    file_format="delta"
)

# Query benefits:
# SELECT * FROM customers_zorder 
# WHERE age_group = 'MILLENNIAL' 
#   AND income_bracket = 'HIGH' 
#   AND region = 'WEST_COAST'
# - Reads minimal files due to multi-dimensional clustering
# - Much faster than traditional single-column partitioning
```

## Method: write_streaming_optimized()

### Purpose
Optimizes data writing for streaming scenarios with checkpointing and incremental processing.

### Parameters
- `streaming_df`: Streaming DataFrame
- `output_path`: Storage path
- `checkpoint_path`: Checkpoint location for fault tolerance
- `file_format`: Storage format
- `trigger_interval`: Processing trigger interval
- `partition_columns`: Optional partitioning for streaming data

**Example Usage:**
```python
# Stream processing with optimized writing
query = writer.write_streaming_optimized(
    streaming_df=kafka_stream_df,
    output_path="/data/streaming/events",
    checkpoint_path="/checkpoints/events",
    file_format="delta",
    trigger_interval="30 seconds",
    partition_columns=["event_date", "event_type"]
)

# Benefits:
# - Fault-tolerant processing with checkpoints
# - Incremental data arrival handling
# - Partition pruning for streaming queries
# - ACID guarantees with Delta Lake
```

## Complete Usage Examples

### Example 1: Data Warehouse ETL Pipeline
```python
# Initialize writer
writer = SparkAdvancedWriter("DataWarehouseETL")

# Step 1: Initial load (fast, no optimization)
writer.write_for_initial_load_optimization(
    df=raw_sales_df,
    output_path="/data/staging/sales_raw", 
    file_format="parquet",
    target_file_size_mb=200
)

# Step 2: Create optimized dimension tables (bucketed)
writer.write_bucketed_table(
    df=customer_dim_df,
    output_path="/data/warehouse/dim_customer",
    table_name="dim_customer",
    bucket_columns=["customer_id"],
    num_buckets=16,
    sort_columns=["customer_id"],
    file_format="parquet"
)

writer.write_bucketed_table(
    df=product_dim_df, 
    output_path="/data/warehouse/dim_product",
    table_name="dim_product",
    bucket_columns=["product_id"],
    num_buckets=12,
    sort_columns=["product_id", "category"],
    file_format="parquet"
)

# Step 3: Create optimized fact table (partitioned + bucketed)
writer.write_bucketed_and_partitioned_table(
    df=sales_fact_df,
    output_path="/data/warehouse/fact_sales",
    table_name="fact_sales", 
    partition_columns=["year", "month"],
    bucket_columns=["customer_id", "product_id"],
    num_buckets=24,
    sort_columns=["customer_id", "sale_date"],
    file_format="parquet"
)
```

### Example 2: Analytics Optimized Tables
```python
# Create Z-ordered Delta table for analytics
writer.write_with_z_ordering(
    df=customer_behavior_df,
    output_path="/data/analytics/customer_behavior_zorder",
    z_order_columns=["age_bracket", "spending_tier", "region", "signup_month"],
    file_format="delta"
)

# Create time-partitioned event data
writer.write_partitioned_table(
    df=user_events_df,
    output_path="/data/analytics/user_events",
    table_name="user_events_partitioned",
    partition_columns=["event_date", "event_category"],
    file_format="delta",
    dynamic_partitioning=True
)
```

### Example 3: Streaming Data Pipeline
```python
# Set up streaming pipeline with optimized writing
streaming_query = writer.write_streaming_optimized(
    streaming_df=real_time_events_df,
    output_path="/data/streaming/real_time_events",
    checkpoint_path="/checkpoints/real_time_events", 
    file_format="delta",
    trigger_interval="1 minute",
    partition_columns=["event_hour"]
)

# Monitor streaming query
streaming_query.awaitTermination()
```

## Performance Comparison

### Write Strategy Performance Matrix

| Strategy | Write Speed | Query Speed | Storage Efficiency | Use Case |
|----------|-------------|-------------|-------------------|----------|
| **Initial Load** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ | Bulk imports, staging |
| **Partitioned** | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Time-series, filtered queries |
| **Bucketed** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | Frequent joins |
| **Combined** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Large fact tables |
| **Z-Ordered** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Multi-dimensional analytics |

### Decision Matrix

| Data Characteristics | Recommended Strategy |
|---------------------|---------------------|
| **Small tables (<1M rows)** | Initial Load |
| **Time-series data** | Partitioned by date |
| **Frequently joined dimensions** | Bucketed |
| **Large fact tables** | Partitioned + Bucketed |
| **Multi-dimensional analytics** | Z-Ordered Delta |
| **Real-time streams** | Streaming Optimized |

This comprehensive documentation covers all writing strategies with practical examples and performance considerations for optimal data engineering outcomes.
