# Spark File Formats Documentation

## Overview
The `spark_file_formats.py` module demonstrates how to optimally read and write different file formats (Parquet, Delta Lake, Apache Iceberg, Hive) for maximum ETL performance and functionality.

## Class: SparkFileFormatHandler

### Initialization

```python
handler = SparkFileFormatHandler(app_name="SparkFileFormats")
```

#### Spark Configuration Options

| Configuration | Value | Purpose | Performance Impact |
|---------------|-------|---------|-------------------|
| `spark.sql.adaptive.enabled` | `"true"` | Enable Adaptive Query Execution | Better file reading optimization |
| `spark.sql.adaptive.coalescePartitions.enabled` | `"true"` | Auto-merge small partitions | Fewer small files |
| `spark.sql.parquet.compression.codec` | `"snappy"` | Default Parquet compression | Balanced speed/compression |
| `spark.sql.parquet.enableVectorizedReader` | `"true"` | Vectorized Parquet reading | 3-5x faster reads |
| `spark.serializer` | `"KryoSerializer"` | Fast serialization | Better performance |

## Method: optimize_parquet_operations()

### Purpose
Optimizes Parquet file operations for maximum performance through compression, block size tuning, and advanced reading features.

### Parameters
- `df`: DataFrame to write
- `output_path`: Path for Parquet files

### Parquet Optimization Options

#### Write Optimization Options
| Option | Value | Purpose | Impact |
|--------|-------|---------|---------|
| `compression` | `"snappy"` | Fast compression/decompression | 2-3x compression, fast I/O |
| `parquet.block.size` | `"134217728"` | 128MB blocks | Optimal for HDFS, better parallelism |
| `parquet.page.size` | `"1048576"` | 1MB pages | Balance memory vs I/O |

#### Read Optimization Options
| Option | Value | Purpose | Impact |
|--------|-------|---------|---------|
| `mergeSchema` | `"false"` | Skip schema merging | Faster reads, avoid schema conflicts |
| `pathGlobFilter` | `"*.parquet"` | Filter files by pattern | Read only relevant files |

### Compression Codec Comparison

| Codec | Compression Ratio | Speed | CPU Usage | Best For |
|-------|------------------|-------|-----------|----------|
| **snappy** | 2-3x | Very Fast | Low | Default choice, balanced |
| **gzip** | 3-4x | Medium | Medium | Storage optimization |
| **lz4** | 2x | Fastest | Very Low | CPU-constrained environments |
| **zstd** | 3-4x | Fast | Medium | Best compression with good speed |
| **brotli** | 4-5x | Slow | High | Maximum compression |

**Example Usage:**
```python
# Sample sales data
sales_data = [
    (1, "2024-01-15", "Electronics", 1500),
    (2, "2024-01-16", "Clothing", 800),
    (3, "2024-01-17", "Books", 25)
]
sales_df = spark.createDataFrame(sales_data, ["id", "date", "category", "amount"])

# Optimize Parquet operations
filtered_df, selected_df = handler.optimize_parquet_operations(sales_df, "/data/parquet")

# Results in:
# - Optimized write: snappy compression, 128MB blocks, 1MB pages
# - Optimized read: predicate pushdown, column pruning
# - Performance: 3-5x faster reads, 50-70% storage savings
```

### Advanced Parquet Features

#### Predicate Pushdown
```python
# Filters pushed down to file level - only reads relevant row groups
filtered_df = spark.read.parquet("/data/sales") \
    .filter(col("date") >= "2024-01-01") \
    .filter(col("amount") > 100)

# Parquet reader skips row groups that don't match filters
# Performance gain: 70-95% less data read
```

#### Column Pruning
```python
# Only reads specified columns from Parquet files
selected_df = spark.read.parquet("/data/sales") \
    .select("customer_id", "amount", "date")

# Columnar format allows reading only needed columns
# Performance gain: 50-90% less I/O depending on column count
```

#### Schema Evolution
```python
# Handle schema changes in Parquet files
evolved_df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("/data/sales")

# Merges schemas from all Parquet files
# Use sparingly - adds overhead to reads
```

### Parquet Best Practices

| Practice | Configuration | Benefit |
|----------|---------------|---------|
| **Optimal Block Size** | 128MB-1GB | Better parallelism |
| **Snappy Compression** | Default | Balanced performance |
| **Vectorized Reading** | Always enabled | 3-5x faster reads |
| **Predicate Pushdown** | Automatic | 70-95% less I/O |
| **Column Pruning** | Select only needed columns | 50-90% less I/O |

## Method: delta_lake_operations()

### Purpose
Demonstrates Delta Lake operations for ACID transactions, time travel, and advanced data lake capabilities.

### Parameters
- `df`: DataFrame to write
- `delta_path`: Path for Delta table

### Delta Lake Key Features

#### 1. ACID Transactions
```python
# Write with ACID guarantees
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save("/data/delta/customers")

# All operations are atomic - either complete or fail entirely
# Concurrent reads/writes are safe
```

#### 2. Schema Evolution
```python
# Automatic schema evolution
new_df = df.withColumn("new_column", lit("default_value"))
new_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/data/delta/customers")

# Delta automatically handles schema changes
# Backward compatibility maintained
```

#### 3. Time Travel
```python
# Read historical versions
historical_df = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-01") \
    .load("/data/delta/customers")

# Or by version number
version_df = spark.read \
    .format("delta") \
    .option("versionAsOf", 10) \
    .load("/data/delta/customers")
```

#### 4. UPSERT Operations (MERGE)
```python
from delta.tables import DeltaTable

# Load existing Delta table
delta_table = DeltaTable.forPath(spark, "/data/delta/customers")

# Upsert new data
delta_table.alias("target").merge(
    new_data.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Efficient upsert operation with ACID guarantees
```

#### 5. Optimize and Vacuum
```python
# Compact small files
delta_table.optimize().executeCompaction()

# Z-order optimization for multi-dimensional queries
delta_table.optimize().executeZOrderBy("customer_id", "region")

# Clean up old files
delta_table.vacuum(168)  # Keep 7 days of history (168 hours)
```

**Example Usage:**
```python
# Sample customer data
customer_data = [
    (1, "John Doe", "Premium", "2024-01-15"),
    (2, "Jane Smith", "Standard", "2024-01-16")
]
customer_df = spark.createDataFrame(customer_data, 
    ["customer_id", "name", "tier", "signup_date"])

# Delta Lake operations
delta_df = handler.delta_lake_operations(customer_df, "/data/delta")

# Benefits:
# - ACID transactions ensure data consistency
# - Time travel enables data recovery and auditing
# - Schema evolution handles changing requirements
# - UPSERT operations simplify data updates
```

### Delta Lake Configuration Options

| Configuration | Default | Purpose | When to Adjust |
|---------------|---------|---------|----------------|
| `delta.autoOptimize.optimizeWrite` | `false` | Auto-optimize during writes | Enable for write-heavy workloads |
| `delta.autoOptimize.autoCompact` | `false` | Auto-compact small files | Enable for frequent small writes |
| `delta.deletedFileRetentionDuration` | `"interval 1 week"` | How long to keep deleted files | Adjust based on time travel needs |
| `delta.logRetentionDuration` | `"interval 30 days"` | Transaction log retention | Longer for audit requirements |

## Method: iceberg_table_operations()

### Purpose
Demonstrates Apache Iceberg table operations for advanced data lake capabilities with hidden partitioning and schema evolution.

### Parameters
- `df`: DataFrame to write
- `iceberg_path`: Path for Iceberg table

### Iceberg Key Features

#### 1. Hidden Partitioning
```python
# Iceberg handles partitioning automatically
df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .option("write.target-file-size-bytes", "134217728") \
    .saveAsTable("catalog.database.table")

# Partitioning is hidden from users but optimizes queries
# No need to manually manage partition columns
```

#### 2. Schema Evolution
```python
# Add columns without rewriting data
ALTER TABLE catalog.database.table 
ADD COLUMN new_field string;

# Rename columns
ALTER TABLE catalog.database.table 
RENAME COLUMN old_name TO new_name;

# Schema changes are metadata-only operations
```

#### 3. Partition Evolution
```python
# Change partitioning strategy without rewriting data
ALTER TABLE catalog.database.table 
REPLACE PARTITION FIELD bucket(customer_id, 10) 
WITH PARTITION FIELD customer_region;

# Iceberg can evolve partition strategy over time
```

#### 4. Time Travel and Snapshots
```python
# Read specific snapshot
snapshot_df = spark.read \
    .format("iceberg") \
    .option("snapshot-id", 12345) \
    .table("catalog.database.table")

# Read as of timestamp
timestamp_df = spark.read \
    .format("iceberg") \
    .option("as-of-timestamp", "1609459200000") \
    .table("catalog.database.table")
```

**Example Usage:**
```python
# Configure Iceberg catalog
spark.conf.set("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.my_catalog.type", "hadoop")
spark.conf.set("spark.sql.catalog.my_catalog.warehouse", "/data/iceberg")

# Sample transaction data
transaction_data = [
    (1, "2024-01-15", "CUST001", 1500),
    (2, "2024-01-16", "CUST002", 800)
]
transaction_df = spark.createDataFrame(transaction_data, 
    ["transaction_id", "date", "customer_id", "amount"])

# Iceberg operations
iceberg_df = handler.iceberg_table_operations(transaction_df, "/data/iceberg")

# Benefits:
# - Hidden partitioning simplifies usage
# - Schema evolution without data rewrites
# - Partition evolution for changing access patterns
# - ACID transactions and time travel
```

### Iceberg vs Delta Lake Comparison

| Feature | Iceberg | Delta Lake | Winner |
|---------|---------|------------|---------|
| **Hidden Partitioning** | ✅ Full support | ❌ Manual partitioning | Iceberg |
| **Schema Evolution** | ✅ Full support | ✅ Full support | Tie |
| **Time Travel** | ✅ Snapshots | ✅ Versions/timestamps | Tie |
| **ACID Transactions** | ✅ Full support | ✅ Full support | Tie |
| **Ecosystem Support** | Growing | Mature | Delta Lake |
| **Cloud Integration** | Good | Excellent | Delta Lake |

## Method: hive_table_operations()

### Purpose
Demonstrates Hive table operations for metadata management and integration with traditional data warehouse tools.

### Parameters
- `df`: DataFrame to write
- `database_name`: Hive database name
- `table_name`: Hive table name

### Hive Table Features

#### 1. Metadata Management
```python
# Create Hive database
spark.sql("CREATE DATABASE IF NOT EXISTS sales_warehouse")

# Create managed table
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .saveAsTable("sales_warehouse.customer_orders")

# Hive metastore manages table metadata
# Tables accessible from other tools (Hive, Impala, Presto)
```

#### 2. Partitioned Tables
```python
# Create partitioned Hive table
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("sales_warehouse.orders_partitioned")

# Directory structure:
# /warehouse/orders_partitioned/year=2024/month=01/
# /warehouse/orders_partitioned/year=2024/month=02/
```

#### 3. External Tables
```python
# Create external table pointing to existing data
spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_warehouse.external_sales
    USING PARQUET
    LOCATION '/data/external/sales'
""")

# Data remains in original location
# Only metadata stored in Hive metastore
```

#### 4. Dynamic Partition Pruning
```python
# Query with partition filters
filtered_df = spark.sql("""
    SELECT customer_id, SUM(amount) as total_sales
    FROM sales_warehouse.orders_partitioned
    WHERE year = 2024 AND month IN (1, 2, 3)
    GROUP BY customer_id
""")

# Only reads relevant partitions
# Massive performance improvement for large tables
```

**Example Usage:**
```python
# Sample order data
order_data = [
    (1, "CUST001", 1500, 2024, 1),
    (2, "CUST002", 800, 2024, 1),
    (3, "CUST001", 1200, 2024, 2)
]
order_df = spark.createDataFrame(order_data, 
    ["order_id", "customer_id", "amount", "year", "month"])

# Hive table operations
hive_df, filtered_hive = handler.hive_table_operations(order_df, "sales_db", "orders")

# Benefits:
# - Metadata management for data discovery
# - Integration with BI tools
# - Partition pruning for large tables
# - ACID transactions (with Hive 3.0+)
```

### Hive Table Types

| Type | Description | Use Case | Performance |
|------|-------------|----------|-------------|
| **Managed** | Spark manages data and metadata | Data warehousing | Good |
| **External** | Points to existing data | Data lake integration | Excellent |
| **Partitioned** | Divided by column values | Large table optimization | Excellent |
| **Bucketed** | Hash-distributed files | Join optimization | Excellent |

## Method: optimize_file_sizes_and_compression()

### Purpose
Optimizes file sizes and compression settings for different formats based on data characteristics.

### File Size Optimization Strategy
```python
# Calculate optimal partitions for target file size
row_count = df.count()
estimated_size_per_row = 500  # bytes (estimated)
target_file_size = 128 * 1024 * 1024  # 128MB target

optimal_partitions = max(1, (row_count * estimated_size_per_row) // target_file_size)

# Repartition for optimal file sizes
optimized_df = df.repartition(optimal_partitions)
```

### Compression Strategy by Format

#### Parquet Compression
```python
# Different compression options for Parquet
compression_configs = {
    "snappy": {"speed": "fast", "ratio": "medium", "cpu": "low"},
    "gzip": {"speed": "slow", "ratio": "high", "cpu": "medium"},
    "lz4": {"speed": "very_fast", "ratio": "low", "cpu": "very_low"},
    "zstd": {"speed": "medium", "ratio": "high", "cpu": "medium"}
}

# Write with different compression
for codec in compression_configs:
    df.write \
        .mode("overwrite") \
        .option("compression", codec) \
        .parquet(f"/data/compressed_{codec}")
```

### File Size Guidelines

| File Size | Performance | Use Case | Issues |
|-----------|-------------|----------|---------|
| **< 1MB** | Poor | Testing only | Metadata overhead |
| **1-128MB** | Good | Small datasets | Good balance |
| **128MB-1GB** | Excellent | Most workloads | Optimal range |
| **> 1GB** | Good | Large scans only | Memory pressure |

## Method: columnar_vs_row_formats()

### Purpose
Compares columnar (Parquet, ORC) vs row-based (Avro, JSON) storage formats.

### Format Comparison

#### Columnar Formats (Analytics Optimized)
```python
# Parquet - Most popular
df.write.mode("overwrite").parquet("/data/columnar_parquet")

# ORC - Optimized for Hive
df.write.mode("overwrite").format("orc").save("/data/columnar_orc")
```

#### Row-Based Formats (Transactional Optimized)
```python
# Avro - Schema evolution friendly
df.write.mode("overwrite").format("avro").save("/data/row_avro")

# JSON - Human readable, flexible
df.write.mode("overwrite").json("/data/row_json")
```

### Format Selection Guide

| Workload Type | Recommended Format | Reason |
|---------------|-------------------|---------|
| **Analytics/OLAP** | Parquet | Column pruning, compression |
| **Hive/Hadoop** | ORC | Better Hive integration |
| **Streaming** | Avro | Schema evolution |
| **APIs/Debugging** | JSON | Human readable |
| **Archival** | Parquet + gzip | Maximum compression |
| **Real-time** | Avro | Fast serialization |

## Complete Usage Examples

### Example 1: Data Lake Architecture
```python
# Initialize file format handler
handler = SparkFileFormatHandler("DataLakeETL")

# Raw data layer - fast ingestion
raw_df = spark.read.json("/raw/streaming_data")
handler.optimize_parquet_operations(raw_df, "/bronze/raw_parquet")

# Curated layer - Delta Lake for ACID
curated_df = spark.read.parquet("/bronze/raw_parquet")
processed_df = curated_df.filter(col("quality_score") > 0.8)
handler.delta_lake_operations(processed_df, "/silver/curated_delta")

# Analytics layer - Iceberg for advanced features  
analytics_df = spark.read.format("delta").load("/silver/curated_delta")
aggregated_df = analytics_df.groupBy("category", "region").sum("revenue")
handler.iceberg_table_operations(aggregated_df, "/gold/analytics_iceberg")

# Reporting layer - Hive for BI tool integration
final_df = spark.read.format("iceberg").load("/gold/analytics_iceberg")
handler.hive_table_operations(final_df, "reporting", "revenue_summary")
```

### Example 2: Performance Optimization Pipeline
```python
# Read large dataset
large_df = spark.read.table("large_transactions")

# Optimize file sizes and compression
handler.optimize_file_sizes_and_compression(large_df, "/optimized")

# Compare read performance across formats
handler.read_performance_comparison("/optimized")

# Results show:
# - Parquet: Fastest for analytics (column pruning)
# - ORC: Good compression, slower than Parquet
# - Avro: Good for full-row access
# - JSON: Slowest, highest storage usage
```

### Example 3: Multi-Format ETL Pipeline
```python
# Source: JSON from APIs
json_df = spark.read.json("/source/api_data")

# Stage 1: Convert to Parquet for processing
handler.optimize_parquet_operations(json_df, "/stage1/parquet")

# Stage 2: Process and store in Delta Lake
processed_df = spark.read.parquet("/stage1/parquet") \
    .filter(col("status") == "valid") \
    .withColumn("processed_date", current_date())

handler.delta_lake_operations(processed_df, "/stage2/delta")

# Stage 3: Create analytics views in Iceberg
analytics_df = spark.read.format("delta").load("/stage2/delta") \
    .groupBy("date", "category").sum("amount")

handler.iceberg_table_operations(analytics_df, "/stage3/iceberg")

# Stage 4: Expose through Hive for BI tools
final_df = spark.read.format("iceberg").load("/stage3/iceberg")
handler.hive_table_operations(final_df, "analytics", "daily_summary")
```

## Performance Benchmarks

### Format Performance Comparison

| Format | Write Speed | Read Speed | Storage Efficiency | Query Performance |
|--------|-------------|------------|-------------------|-------------------|
| **Parquet** | Medium | Fast | Excellent | Excellent |
| **ORC** | Medium | Medium | Excellent | Good |
| **Delta Lake** | Medium | Fast | Excellent | Excellent |
| **Iceberg** | Medium | Fast | Excellent | Excellent |
| **Avro** | Fast | Medium | Good | Poor |
| **JSON** | Fast | Slow | Poor | Poor |

This comprehensive documentation covers all file format optimization techniques with practical examples and performance considerations for different use cases.
