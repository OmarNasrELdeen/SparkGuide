# Spark Memory Optimization Documentation

## Overview
The `spark_memory_optimization.py` module demonstrates advanced memory management techniques for large-scale ETL processing, including caching strategies, memory-efficient operations, and garbage collection optimization.

## Class: SparkMemoryOptimizer

### Initialization

```python
optimizer = SparkMemoryOptimizer(app_name="SparkMemoryOptimizer")
```

#### Memory-Specific Spark Configuration Options

| Configuration | Value | Purpose | Impact |
|---------------|-------|---------|---------|
| `spark.executor.memory` | `"4g"` | Total executor memory | Base memory allocation |
| `spark.executor.memoryFraction` | `"0.8"` | Fraction for execution/storage | 80% for Spark, 20% for system |
| `spark.executor.memoryStorageFraction` | `"0.5"` | Storage vs execution memory split | 50/50 split between caching and processing |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | `"10000"` | Arrow batch size | Memory vs performance trade-off |
| `spark.serializer` | `"KryoSerializer"` | Serialization method | Faster, more memory efficient |
| `spark.kryo.registrationRequired` | `"false"` | Kryo class registration | Flexibility vs performance |

### Memory Architecture Understanding

#### Spark Memory Regions
1. **Reserved Memory**: 300MB reserved for system operations
2. **User Memory**: (Total - Reserved) × (1 - memoryFraction) for user data structures
3. **Spark Memory**: (Total - Reserved) × memoryFraction split between:
   - **Storage Memory**: For caching DataFrames/RDDs
   - **Execution Memory**: For joins, aggregations, shuffles

#### Memory Configuration Best Practices
```python
# Calculate optimal memory settings
total_memory = "8g"  # 8GB per executor
reserved = 300  # MB
user_fraction = 0.2  # 20% for user code
storage_fraction = 0.5  # 50% of Spark memory for storage

spark_memory = (8192 - 300) * 0.8  # 6313.6 MB
storage_memory = 6313.6 * 0.5      # 3156.8 MB for caching
execution_memory = 6313.6 * 0.5    # 3156.8 MB for processing
```

## Method: analyze_memory_usage()

### Purpose
Analyzes current memory usage patterns and provides insights for optimization.

### Parameters
- `df`: DataFrame to analyze
- `description`: Description for logging

### Memory Analysis Metrics

#### Key Metrics Collected
1. **Storage Level**: Current caching configuration
2. **Row Count**: Total number of records
3. **Partition Count**: Data distribution
4. **Estimated Size**: Rough memory footprint calculation

**Example Usage:**
```python
# Analyze large sales dataset
sales_df = spark.read.table("large_sales_fact")
memory_stats = optimizer.analyze_memory_usage(sales_df, "Sales Fact Table")

# Output Analysis:
# === Memory Analysis for Sales Fact Table ===
# Storage Level: StorageLevel(False, False, False, False, 1)
# Is Cached: False
# Rows: 50,000,000
# Partitions: 200
# Avg rows per partition: 250,000
# Estimated size: 3,750.00 MB

# Use analysis to make caching decisions
if memory_stats["estimated_size_mb"] < 2000:  # < 2GB
    sales_df.cache()  # Safe to cache in memory
elif memory_stats["estimated_size_mb"] < 10000:  # < 10GB
    sales_df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Use serialized storage
else:
    # Process in chunks or use disk-based operations
    pass
```

### Memory Size Estimation Formula
```python
def estimate_dataframe_size(df):
    """Estimate DataFrame memory usage"""
    row_count = df.count()
    column_count = len(df.columns)
    
    # Rough estimation based on data types
    avg_bytes_per_field = {
        'IntegerType': 4,
        'LongType': 8,
        'DoubleType': 8,
        'StringType': 20,  # Average string length
        'TimestampType': 8,
        'BooleanType': 1
    }
    
    total_bytes = 0
    for field in df.schema.fields:
        field_bytes = avg_bytes_per_field.get(str(field.dataType), 20)
        total_bytes += row_count * field_bytes
    
    size_mb = total_bytes / (1024 * 1024)
    return size_mb
```

## Method: optimize_caching_strategies()

### Purpose
Demonstrates different caching strategies for various scenarios and requirements.

### Storage Levels Explained

#### 1. MEMORY_ONLY
```python
# Fastest access, highest risk
df.persist(StorageLevel.MEMORY_ONLY)
```
- **Speed**: Fastest (no serialization, no disk I/O)
- **Memory Usage**: Highest (uncompressed objects)
- **Fault Tolerance**: Lowest (data lost if executor fails)
- **Use Case**: Small datasets, memory-rich clusters

#### 2. MEMORY_AND_DISK
```python
# Balanced approach (default)
df.persist(StorageLevel.MEMORY_AND_DISK)
```
- **Speed**: Fast (memory) + Medium (disk fallback)
- **Memory Usage**: High
- **Fault Tolerance**: High (disk backup)
- **Use Case**: General purpose, production workloads

#### 3. MEMORY_ONLY_SER
```python
# Memory efficient
df.persist(StorageLevel.MEMORY_ONLY_SER)
```
- **Speed**: Medium (serialization overhead)
- **Memory Usage**: Medium (compressed)
- **Fault Tolerance**: Low (memory only)
- **Use Case**: Memory-constrained but fast storage needed

#### 4. MEMORY_AND_DISK_SER
```python
# Production recommended
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```
- **Speed**: Medium (serialization) + Medium (disk)
- **Memory Usage**: Medium (compressed)
- **Fault Tolerance**: High (disk backup + serialized)
- **Use Case**: Production environments, large datasets

#### 5. DISK_ONLY
```python
# Memory constrained environments
df.persist(StorageLevel.DISK_ONLY)
```
- **Speed**: Slow (disk I/O only)
- **Memory Usage**: Minimal
- **Fault Tolerance**: High (persistent storage)
- **Use Case**: Very memory-constrained clusters

#### 6. Replicated Storage Levels
```python
# High availability
df.persist(StorageLevel.MEMORY_AND_DISK_2)  # 2x replication
```
- **Speed**: Same as base level
- **Memory Usage**: 2x storage requirement
- **Fault Tolerance**: Very High (multiple copies)
- **Use Case**: Critical data, high-availability requirements

**Example Usage:**
```python
# Sample customer data analysis
customer_df = spark.read.table("customers")

# Get all caching strategies
cached_versions = optimizer.optimize_caching_strategies(customer_df)

# Choose strategy based on requirements
if cluster_memory_abundant:
    analysis_df = cached_versions["memory_only"]
elif production_environment:
    analysis_df = cached_versions["memory_disk"]
elif memory_constrained:
    analysis_df = cached_versions["disk_only"]
elif high_availability_needed:
    analysis_df = cached_versions["replicated"]

# Always unpersist when done
analysis_df.unpersist()
```

### Caching Strategy Decision Matrix

| Scenario | Recommended Storage | Reason |
|----------|-------------------|---------|
| **Small datasets (<500MB)** | MEMORY_ONLY | Fast access, fits in memory |
| **Medium datasets (500MB-5GB)** | MEMORY_AND_DISK_SER | Balanced performance |
| **Large datasets (>5GB)** | MEMORY_AND_DISK_SER | Efficient use of memory |
| **Memory constrained** | DISK_ONLY | Preserve memory for processing |
| **Critical production data** | MEMORY_AND_DISK_2 | High availability |
| **Development/Testing** | MEMORY_ONLY | Speed over reliability |

## Method: memory_efficient_operations()

### Purpose
Demonstrates memory-efficient operations for processing large datasets without running out of memory.

### Memory-Efficient Techniques

#### 1. Iterative Processing (Chunking)
```python
def process_large_dataset_in_chunks(df, chunk_size=100000):
    """Process large datasets in manageable chunks"""
    total_rows = df.count()
    results = []
    
    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        
        # Process chunk
        chunk = df.filter((col("id") >= start) & (col("id") < end))
        chunk_result = chunk.groupBy("category").sum("amount")
        
        # Collect small result
        chunk_data = chunk_result.collect()
        results.extend(chunk_data)
        
        # Clean up chunk
        chunk.unpersist() if chunk.is_cached else None
    
    # Combine results
    return spark.createDataFrame(results, chunk_result.schema)
```

#### 2. Sampling for Large Dataset Analysis
```python
# Use sampling for exploratory analysis
def memory_efficient_analysis(large_df, sample_fraction=0.1):
    """Analyze large datasets using sampling"""
    
    # Sample data for analysis
    sample_df = large_df.sample(sample_fraction, seed=42)
    sample_df.cache()  # Cache the smaller sample
    
    # Perform analysis on sample
    stats = sample_df.describe()
    distributions = sample_df.groupBy("category").count()
    
    # Scale results back to full dataset
    estimated_total = sample_df.count() / sample_fraction
    
    sample_df.unpersist()
    return stats, distributions, estimated_total
```

#### 3. Columnar Operations Instead of Row-wise
```python
# Efficient: Vectorized operations
efficient_df = large_df \
    .select("customer_id", "amount", "date") \
    .filter(col("amount") > 100) \
    .withColumn("amount_category", 
               when(col("amount") > 1000, "high")
               .when(col("amount") > 500, "medium")
               .otherwise("low")) \
    .groupBy("amount_category").sum("amount")

# Avoid: Row-wise operations with UDFs
from pyspark.sql.types import StringType

def categorize_amount(amount):  # This creates Python UDF overhead
    if amount > 1000: return "high"
    elif amount > 500: return "medium"
    else: return "low"

categorize_udf = udf(categorize_amount, StringType())
# less_efficient_df = large_df.withColumn("category", categorize_udf(col("amount")))
```

#### 4. Avoid Wide Transformations When Possible
```python
# Memory efficient: Narrow transformations first
optimized_pipeline = large_df \
    .filter(col("status") == "active") \      # Narrow: filter
    .select("customer_id", "amount", "date") \ # Narrow: projection
    .withColumn("year", year(col("date"))) \   # Narrow: column operation
    .groupBy("customer_id", "year") \          # Wide: but on filtered data
    .sum("amount")

# Less efficient: Wide transformations on full dataset
# less_efficient = large_df.groupBy("customer_id").sum("amount").filter(col("sum(amount)") > 1000)
```

**Example Usage:**
```python
# Large transaction dataset (100M+ rows)
large_transactions = spark.read.table("transaction_history")

# Apply memory-efficient operations
sample_df, efficient_df, chunked_results = optimizer.memory_efficient_operations(large_transactions)

# Memory-efficient analysis workflow:
# 1. Sample for quick insights
# 2. Process efficiently with vectorized operations  
# 3. Use chunking for operations that don't fit in memory
```

## Method: garbage_collection_optimization()

### Purpose
Optimizes garbage collection for long-running Spark jobs to prevent memory issues.

### GC Optimization Strategies

#### 1. Managed Cache Operations
```python
def managed_cache_workflow(df):
    """Properly manage DataFrame caching lifecycle"""
    
    # Cache DataFrame
    df.cache()
    
    try:
        # Perform multiple operations
        result1 = df.filter(col("amount") > 100).count()
        result2 = df.groupBy("category").sum("amount").collect()
        result3 = df.orderBy("amount").limit(100).collect()
        
        return result1, result2, result3
    
    finally:
        # Always unpersist, even if operations fail
        df.unpersist()
```

#### 2. Periodic Cleanup in Long Jobs
```python
def long_running_job_with_cleanup(datasets, process_func):
    """Long-running job with periodic memory cleanup"""
    
    results = []
    cache_registry = []
    
    for i, dataset in enumerate(datasets):
        # Process dataset
        result = process_func(dataset)
        results.append(result)
        
        # Track cached DataFrames
        if dataset.is_cached:
            cache_registry.append(dataset)
        
        # Periodic cleanup every 10 iterations
        if i % 10 == 0 and i > 0:
            print(f"Performing cleanup at iteration {i}")
            
            # Unpersist old cached DataFrames
            for cached_df in cache_registry:
                cached_df.unpersist()
            cache_registry.clear()
            
            # Force garbage collection (optional)
            import gc
            gc.collect()
    
    return results
```

#### 3. Memory Leak Prevention
```python
def prevent_memory_leaks():
    """Best practices to prevent memory leaks"""
    
    # 1. Always unpersist cached DataFrames
    df.cache()
    try:
        # ... operations ...
        pass
    finally:
        df.unpersist()
    
    # 2. Avoid accumulating large lists in driver
    # Bad: results = [large_df.collect() for df in dataframes]
    # Good: Process and aggregate incrementally
    
    # 3. Clear broadcast variables when done
    broadcast_var = spark.sparkContext.broadcast(large_lookup)
    try:
        # ... use broadcast_var ...
        pass
    finally:
        broadcast_var.unpersist()
    
    # 4. Limit DataFrame lineage depth
    if df.rdd.getNumPartitions() > 1000:  # Long lineage indicator
        df.checkpoint()  # Break lineage chain
```

## Method: spill_and_shuffle_optimization()

### Purpose
Optimizes operations that cause memory spills and shuffles for better performance.

### Spill and Shuffle Optimization Techniques

#### 1. Join Optimization to Reduce Shuffles
```python
def optimize_joins_for_memory(large_df, medium_df, small_df):
    """Optimize joins to minimize memory usage"""
    
    # Strategy 1: Broadcast small tables
    if small_df.count() < 1000000:  # < 1M rows
        result = large_df.join(broadcast(small_df), "key")
        print("Using broadcast join - no shuffle for large table")
    
    # Strategy 2: Pre-partition for joins
    else:
        # Partition both DataFrames by join key
        large_partitioned = large_df.repartition(col("join_key"))
        medium_partitioned = medium_df.repartition(col("join_key"))
        
        result = large_partitioned.join(medium_partitioned, "join_key")
        print("Pre-partitioned tables for efficient join")
    
    return result
```

#### 2. Aggregation Optimization
```python
def optimize_aggregations_for_memory(df):
    """Optimize aggregations to reduce memory pressure"""
    
    # Strategy 1: Two-phase aggregation for high cardinality
    if df.select("group_column").distinct().count() > 1000000:
        # Pre-aggregate with partial grouping
        pre_agg = df \
            .repartition(col("group_column")) \
            .groupBy("group_column", "sub_group") \
            .sum("value")
        
        # Final aggregation
        final_agg = pre_agg \
            .groupBy("group_column") \
            .sum("sum(value)")
    
    # Strategy 2: Use approximate aggregations for large datasets
    else:
        final_agg = df \
            .groupBy("group_column") \
            .agg(
                sum("value").alias("total"),
                approx_count_distinct("customer_id").alias("approx_customers")
            )
    
    return final_agg
```

#### 3. Sort Optimization
```python
def memory_efficient_sorting(df, sort_columns):
    """Optimize sorting operations for memory efficiency"""
    
    # Strategy 1: Use repartitionByRange for ordered data
    if len(sort_columns) == 1:
        sorted_df = df.repartitionByRange(10, col(sort_columns[0]))
        print("Using range partitioning for efficient sorting")
    
    # Strategy 2: Sort within partitions when possible
    else:
        sorted_df = df.sortWithinPartitions(*sort_columns)
        print("Sorting within partitions to avoid global shuffle")
    
    return sorted_df
```

## Method: monitor_memory_metrics()

### Purpose
Monitors Spark memory metrics for performance tuning and issue diagnosis.

### Memory Monitoring Techniques

#### 1. Executor Memory Information
```python
def get_executor_memory_stats():
    """Get detailed executor memory statistics"""
    
    sc = spark.sparkContext
    executor_infos = sc.statusTracker().getExecutorInfos()
    
    memory_stats = []
    total_memory = 0
    total_used = 0
    
    for executor in executor_infos:
        stats = {
            "executor_id": executor.executorId,
            "max_memory_gb": executor.maxMemory / (1024**3),
            "memory_used_gb": executor.memoryUsed / (1024**3),
            "memory_utilization": (executor.memoryUsed / executor.maxMemory * 100) if executor.maxMemory > 0 else 0,
            "active_tasks": executor.activeTasks,
            "total_tasks": executor.totalTasks,
            "failed_tasks": executor.failedTasks
        }
        memory_stats.append(stats)
        total_memory += executor.maxMemory
        total_used += executor.memoryUsed
    
    cluster_stats = {
        "total_executors": len(executor_infos),
        "total_memory_gb": total_memory / (1024**3),
        "total_used_gb": total_used / (1024**3),
        "cluster_utilization": (total_used / total_memory * 100) if total_memory > 0 else 0
    }
    
    return memory_stats, cluster_stats
```

#### 2. Storage Memory Analysis
```python
def analyze_storage_memory():
    """Analyze cached DataFrame memory usage"""
    
    storage_status = spark.sparkContext.statusTracker().getStorageStatus()
    
    storage_stats = []
    for storage in storage_status:
        stats = {
            "block_manager_id": storage.blockManagerId,
            "max_memory_mb": storage.maxMem / (1024**2),
            "memory_used_mb": storage.memoryUsed / (1024**2),
            "disk_used_mb": storage.diskUsed / (1024**2),
            "memory_remaining_mb": storage.memoryRemaining / (1024**2)
        }
        storage_stats.append(stats)
    
    return storage_stats
```

## Complete Usage Examples

### Example 1: Memory-Optimized ETL Pipeline
```python
# Initialize memory optimizer
optimizer = SparkMemoryOptimizer("MemoryOptimizedETL")

# Large dataset processing
large_sales_df = spark.read.table("sales_fact")  # 100M+ rows

# Step 1: Analyze memory requirements
memory_analysis = optimizer.analyze_memory_usage(large_sales_df, "Sales Fact")

# Step 2: Choose appropriate caching strategy
if memory_analysis["estimated_size_mb"] < 2000:
    cached_df = large_sales_df.persist(StorageLevel.MEMORY_ONLY)
else:
    cached_df = large_sales_df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Step 3: Memory-efficient processing
efficient_result = optimizer.memory_efficient_operations(cached_df)

# Step 4: Monitor memory usage
executor_stats, cluster_stats = optimizer.monitor_memory_metrics()

# Step 5: Cleanup
cached_df.unpersist()
```

### Example 2: Long-Running Job with Memory Management
```python
# Process multiple datasets with memory management
datasets = [f"table_{i}" for i in range(100)]  # 100 tables to process

def process_table(table_name):
    df = spark.read.table(table_name)
    # Apply memory-efficient operations
    return optimizer.memory_efficient_operations(df)

# Process with periodic cleanup
results = optimizer.garbage_collection_optimization(datasets, process_table)
```

### Memory Optimization Checklist

| Optimization Area | Action | Expected Benefit |
|------------------|--------|------------------|
| **Caching Strategy** | Choose appropriate storage level | 2-10x faster repeated access |
| **Memory Analysis** | Monitor DataFrame sizes | Prevent OOM errors |
| **Iterative Processing** | Process large datasets in chunks | Handle unlimited data sizes |
| **GC Optimization** | Manage cache lifecycle | Prevent memory leaks |
| **Shuffle Reduction** | Optimize joins and aggregations | 30-70% memory reduction |

This comprehensive documentation covers all memory optimization techniques with practical examples for efficient large-scale data processing.
