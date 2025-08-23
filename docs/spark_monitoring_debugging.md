# Spark Monitoring and Debugging Documentation

## Overview
The `spark_monitoring_debugging.py` module provides comprehensive tools for monitoring Spark applications, debugging performance issues, and analyzing execution patterns for optimization.

## Class: SparkMonitoringDebugger

### Initialization

```python
monitor = SparkMonitoringDebugger(app_name="SparkMonitoring")
```

#### Monitoring-Specific Spark Configuration Options

| Configuration | Value | Purpose | Impact |
|---------------|-------|---------|---------|
| `spark.eventLog.enabled` | `"true"` | Enable event logging | Historical analysis via Spark History Server |
| `spark.eventLog.dir` | `"/tmp/spark-events"` | Event log storage location | Persistent monitoring data |
| `spark.history.ui.port` | `"18080"` | History server port | Web UI access for historical analysis |
| `spark.sql.execution.arrow.pyspark.enabled` | `"true"` | Enable Arrow for pandas | Better performance monitoring |
| `spark.serializer` | `"KryoSerializer"` | Fast serialization | Reduced serialization overhead |

### Event Logging Benefits
- **Historical Analysis**: Review past job performance
- **Trend Analysis**: Identify performance patterns over time
- **Debugging**: Analyze failed jobs and bottlenecks
- **Capacity Planning**: Understand resource usage patterns

## Method: performance_profiling()

### Purpose
Profiles DataFrame operations for detailed performance analysis including execution plans, timing, and resource utilization.

### Parameters
- `df`: DataFrame to profile
- `operation_name`: Descriptive name for the operation

### Performance Metrics Collected

#### Execution Plan Analysis
```python
# Logical plan analysis
df.explain(True)  # Shows logical, optimized, and physical plans

# Cost-based analysis
df.explain("cost")  # Shows cost estimates for operations

# Formatted output
df.explain("formatted")  # Human-readable execution plan
```

#### Key Performance Metrics
1. **Execution Time**: Total operation duration
2. **Row Count**: Number of records processed
3. **Partition Distribution**: Load balancing analysis
4. **Throughput**: Records processed per second

**Example Usage:**
```python
# Profile a complex aggregation operation
sales_df = spark.read.table("large_sales_table")
complex_query = sales_df \
    .filter(col("amount") > 100) \
    .groupBy("region", "product_category") \
    .agg(sum("amount").alias("total_sales"),
         countDistinct("customer_id").alias("unique_customers")) \
    .orderBy(desc("total_sales"))

# Profile the operation
metrics = monitor.performance_profiling(complex_query, "Sales Analysis Query")

# Output Analysis:
# === Performance Profiling: Sales Analysis Query ===
# Logical Plan: [Shows query optimization steps]
# Physical Plan: [Shows actual execution strategy]
# Performance Metrics:
#   operation: Sales Analysis Query
#   execution_time: 45.23
#   row_count: 156789
#   partition_count: 20
#   min_partition_size: 6543
#   max_partition_size: 9876
#   avg_partition_size: 7839.45
#   throughput_rows_per_sec: 3467.82
```

### Performance Analysis Interpretation

| Metric | Good Range | Warning Signs | Action Required |
|--------|------------|---------------|-----------------|
| **Throughput** | >1000 rows/sec | <100 rows/sec | Optimize query/partitioning |
| **Partition Skew** | Ratio <2.0 | Ratio >5.0 | Repartition data |
| **Execution Time** | Stable/predictable | Highly variable | Investigate resource contention |

## Method: analyze_query_execution()

### Purpose
Analyzes different query execution patterns to identify bottlenecks and optimization opportunities.

### Query Pattern Analysis

#### Query Types Analyzed
1. **Full Scan**: `SELECT COUNT(*) FROM table`
2. **Filtered Scan**: `SELECT COUNT(*) FROM table WHERE condition`
3. **Aggregation**: `SELECT category, SUM(amount) FROM table GROUP BY category`
4. **Window Function**: Complex analytical queries with ranking/partitioning

#### Execution Plan Types
```python
# Different explain modes for comprehensive analysis
result_df.explain("simple")     # Basic physical plan
result_df.explain("extended")   # Logical + physical plans
result_df.explain("codegen")    # Generated code analysis
result_df.explain("cost")       # Cost-based optimization details
result_df.explain("formatted")  # Human-readable format
```

**Example Usage:**
```python
# Analyze query execution patterns
customer_df = spark.read.table("customer_transactions")
query_metrics = monitor.analyze_query_execution(customer_df, "Customer Analysis")

# Results show performance comparison:
# FULL_SCAN Query Plan: [Execution strategy for full table scan]
# FILTERED_SCAN Query Plan: [Shows predicate pushdown optimization]
# AGGREGATION Query Plan: [Displays grouping and shuffling strategy]
# WINDOW_FUNCTION Query Plan: [Complex analytical processing plan]
#
# Query Metrics:
# {
#   "full_scan": {"execution_time": 12.34, "result_count": 1000000},
#   "filtered_scan": {"execution_time": 3.45, "result_count": 250000},
#   "aggregation": {"execution_time": 8.67, "result_count": 50},
#   "window_function": {"execution_time": 15.23, "result_count": 1000000}
# }
```

### Query Optimization Insights

| Query Type | Optimization Focus | Performance Indicators |
|------------|-------------------|----------------------|
| **Full Scan** | Column pruning, partition pruning | I/O throughput |
| **Filtered Scan** | Predicate pushdown, indexing | Filter selectivity |
| **Aggregation** | Shuffle optimization, pre-aggregation | Shuffle data volume |
| **Window Function** | Partition optimization, memory usage | Memory spill events |

## Method: monitor_resource_usage()

### Purpose
Monitors Spark cluster resource usage including CPU, memory, and task distribution across executors.

### Resource Monitoring Categories

#### Application-Level Metrics
```python
app_info = status_tracker.getApplicationInfo()
# Provides:
# - Application ID and name
# - Start time and duration
# - Application state
```

#### Executor-Level Metrics
```python
executor_infos = status_tracker.getExecutorInfos()
# For each executor:
# - CPU cores (total and active)
# - Memory allocation and usage
# - Task execution statistics
# - Failed task counts
```

#### Cluster-Level Aggregations
```python
cluster_metrics = {
    "total_cores": sum(executor.totalCores for executor in executors),
    "total_memory_gb": sum(executor.maxMemory for executor in executors) / (1024**3),
    "memory_utilization": (total_used / total_available) * 100,
    "executor_count": len(executors)
}
```

**Example Usage:**
```python
# Monitor cluster resource usage
cluster_metrics = monitor.monitor_resource_usage()

# Output Analysis:
# === Resource Usage Monitoring ===
# Application ID: app-20240815-123456-0001
# Application Name: SparkMonitoring
# Application Start Time: 1692123456789
#
# Executor Information (4 executors):
#   Executor driver:
#     Cores: 4
#     Memory: 2.00 GB
#     Memory Used: 0.45 GB
#     Active Tasks: 0
#     Failed Tasks: 0
#   Executor 1:
#     Cores: 4
#     Memory: 4.00 GB
#     Memory Used: 1.23 GB
#     Active Tasks: 2
#     Failed Tasks: 0
#
# Cluster Summary:
#   total_cores: 16
#   total_memory_gb: 14.00
#   total_memory_used_gb: 3.45
#   memory_utilization_percent: 24.64
#   executor_count: 4
```

### Resource Utilization Analysis

| Metric | Optimal Range | Warning Threshold | Critical Threshold |
|--------|---------------|-------------------|-------------------|
| **Memory Utilization** | 60-80% | >85% | >95% |
| **CPU Utilization** | 70-90% | >95% | 100% sustained |
| **Failed Tasks** | 0-1% | >5% | >10% |
| **Task Skew** | Even distribution | 2x variance | 5x variance |

## Method: debug_data_skew()

### Purpose
Identifies and analyzes data skew issues that cause uneven processing and performance bottlenecks.

### Parameters
- `df`: DataFrame to analyze
- `partition_column`: Column suspected of causing skew

### Skew Detection Methodology

#### Statistical Skew Analysis
```python
# Analyze partition distribution
partition_stats = df \
    .groupBy(partition_column) \
    .count() \
    .orderBy(desc("count"))

# Calculate skew metrics
stats = partition_stats.agg(
    min("count").alias("min_size"),
    max("count").alias("max_size"),
    avg("count").alias("avg_size"),
    stddev("count").alias("stddev_size")
).collect()[0]

skew_ratio = stats["max_size"] / stats["avg_size"]
coefficient_of_variation = stats["stddev_size"] / stats["avg_size"]
```

#### Skew Classification
```python
def classify_skew(skew_ratio):
    if skew_ratio < 1.5:
        return "No Skew", "Excellent distribution"
    elif skew_ratio < 2.5:
        return "Mild Skew", "Monitor performance"
    elif skew_ratio < 5.0:
        return "Moderate Skew", "Consider optimization"
    else:
        return "High Skew", "Immediate action required"
```

**Example Usage:**
```python
# Analyze customer transaction skew
transactions_df = spark.read.table("customer_transactions")
skew_analysis = monitor.debug_data_skew(transactions_df, "customer_id")

# Output Analysis:
# === Data Skew Analysis for 'customer_id' ===
# Top 10 partition sizes:
# +----------+-------+
# |customer_id| count|
# +----------+-------+
# |    CUST001|1500000| ← Mega customer causing skew
# |    CUST002| 890000| ← Large customer
# |    CUST003|   1234| ← Normal customer
# |    CUST004|    987| ← Normal customer
# +----------+-------+
#
# Skew Analysis:
#   Min partition size: 123
#   Max partition size: 1500000
#   Average partition size: 15678.45
#   Standard deviation: 145623.78
#   Skew ratio (max/avg): 95.67
#   Coefficient of variation: 9.28
# HIGH SKEW DETECTED! Recommendations:
#   1. Consider salting the skewed keys
#   2. Use different partitioning strategy
#   3. Filter out or handle skewed values separately
```

### Skew Mitigation Strategies

| Skew Level | Strategy | Implementation | Performance Gain |
|------------|----------|----------------|------------------|
| **Mild (1.5-2.5)** | Monitor | No immediate action | N/A |
| **Moderate (2.5-5.0)** | Repartitioning | `df.repartition(col("key"))` | 2-5x improvement |
| **High (>5.0)** | Salting | Add random salt to keys | 5-20x improvement |
| **Extreme (>10.0)** | Separate processing | Handle skewed values differently | 10-50x improvement |

## Method: bottleneck_identification()

### Purpose
Identifies performance bottlenecks in ETL pipelines by analyzing operation-specific performance.

### Parameters
- `df`: DataFrame to analyze
- `operations`: Dictionary of operation names and functions

### Bottleneck Analysis Process

#### Operation Performance Profiling
```python
def analyze_operation_performance(df, operations):
    """Profile multiple operations to identify bottlenecks"""
    
    bottleneck_results = {}
    
    for operation_name, operation_func in operations.items():
        # Measure operation performance
        start_time = time.time()
        result_df = operation_func(df)
        
        # Force execution and measure
        if hasattr(result_df, 'count'):
            count = result_df.count()
        else:
            count = len(result_df.collect())
        
        execution_time = time.time() - start_time
        
        bottleneck_results[operation_name] = {
            "execution_time": execution_time,
            "result_count": count,
            "throughput": count / execution_time if execution_time > 0 else 0
        }
    
    return bottleneck_results
```

#### Bottleneck Ranking
```python
# Sort operations by execution time (slowest first)
sorted_operations = sorted(bottleneck_results.items(), 
                         key=lambda x: x[1]["execution_time"], 
                         reverse=True)

# Identify top bottlenecks
for operation, metrics in sorted_operations:
    print(f"{operation}: {metrics['execution_time']:.2f}s "
          f"({metrics['throughput']:.0f} rows/sec)")
```

**Example Usage:**
```python
# Define operations to analyze
operations = {
    "filter_operation": lambda x: x.filter(col("amount") > 500),
    "join_operation": lambda x: x.join(dimension_table, "category"),
    "groupby_operation": lambda x: x.groupBy("region").sum("amount"),
    "window_operation": lambda x: x.withColumn("rank", 
        row_number().over(Window.partitionBy("category").orderBy("amount")))
}

# Identify bottlenecks
bottlenecks = monitor.bottleneck_identification(sales_df, operations)

# Results show performance ranking:
# Bottleneck Analysis (slowest first):
#   join_operation: 45.67s (34567 rows/sec)
#   window_operation: 23.45s (67890 rows/sec)
#   groupby_operation: 12.34s (128764 rows/sec)
#   filter_operation: 3.21s (498456 rows/sec)
```

### Bottleneck Optimization Strategies

| Bottleneck Type | Common Causes | Optimization Approach |
|----------------|---------------|----------------------|
| **Joins** | Shuffle overhead, data skew | Broadcast joins, bucketing |
| **Aggregations** | High cardinality, skew | Pre-aggregation, salting |
| **Window Functions** | Large partitions | Partition optimization |
| **Filters** | Non-selective predicates | Predicate pushdown |

## Method: memory_leak_detection()

### Purpose
Detects potential memory leaks in iterative Spark operations through memory usage pattern analysis.

### Parameters
- `df_operations`: Dictionary of operations that create DataFrames
- `iterations`: Number of iterations to test

### Memory Leak Detection Process

#### Iterative Memory Monitoring
```python
def detect_memory_leaks(operations, iterations=5):
    """Monitor memory usage across multiple iterations"""
    
    sc = spark.sparkContext
    memory_usage = []
    
    for i in range(iterations):
        # Get baseline memory
        executor_infos = sc.statusTracker().getExecutorInfos()
        memory_before = sum(e.memoryUsed for e in executor_infos)
        
        # Perform operations
        for operation_name, operation_func in operations.items():
            temp_df = operation_func()
            if hasattr(temp_df, 'count'):
                _ = temp_df.count()  # Force execution
            
            # Clean up
            if hasattr(temp_df, 'unpersist'):
                temp_df.unpersist()
        
        # Measure memory after operations
        executor_infos = sc.statusTracker().getExecutorInfos()
        memory_after = sum(e.memoryUsed for e in executor_infos)
        
        memory_usage.append({
            "iteration": i + 1,
            "memory_before_mb": memory_before / (1024**2),
            "memory_after_mb": memory_after / (1024**2),
            "memory_delta_mb": (memory_after - memory_before) / (1024**2)
        })
    
    return memory_usage
```

#### Leak Pattern Analysis
```python
# Analyze memory usage trends
total_delta = memory_usage[-1]["memory_after_mb"] - memory_usage[0]["memory_before_mb"]
avg_delta_per_iteration = sum(m["memory_delta_mb"] for m in memory_usage) / len(memory_usage)

# Classification
if avg_delta_per_iteration > 100:  # 100MB per iteration
    leak_severity = "Critical - Potential memory leak detected"
elif avg_delta_per_iteration > 50:
    leak_severity = "Warning - Monitor memory usage"
else:
    leak_severity = "Normal - Memory usage appears stable"
```

**Example Usage:**
```python
# Define operations that might leak memory
leak_test_operations = {
    "cache_operation": lambda: large_df.cache(),
    "join_operation": lambda: large_df.join(broadcast(small_df), "key"),
    "aggregation": lambda: large_df.groupBy("category").sum("amount")
}

# Test for memory leaks
memory_analysis = monitor.memory_leak_detection(leak_test_operations, iterations=10)

# Output Analysis:
# === Memory Leak Detection (10 iterations) ===
# Iteration 1: Memory delta: 45.23 MB
# Iteration 2: Memory delta: 12.67 MB
# Iteration 3: Memory delta: 156.89 MB  ← Potential leak
# ...
# 
# Memory Leak Analysis:
#   Total memory change: 234.56 MB
#   Average change per iteration: 23.46 MB
#   WARNING: Monitor memory usage
```

## Method: generate_performance_report()

### Purpose
Generates comprehensive performance reports with all collected metrics for analysis and optimization planning.

### Report Components

#### Performance Report Structure
```python
def generate_comprehensive_report(app_metrics):
    """Create detailed performance report"""
    
    report = {
        "report_metadata": {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "application_name": spark.sparkContext.appName,
            "application_id": spark.sparkContext.applicationId,
            "spark_version": spark.version
        },
        "performance_metrics": app_metrics,
        "recommendations": generate_recommendations(app_metrics),
        "trend_analysis": analyze_performance_trends(app_metrics)
    }
    
    return report
```

#### Automated Recommendations
```python
def generate_recommendations(metrics):
    """Generate optimization recommendations based on metrics"""
    
    recommendations = []
    
    # Memory optimization recommendations
    if metrics.get("memory_utilization", 0) > 85:
        recommendations.append({
            "type": "Memory",
            "priority": "High",
            "recommendation": "Increase executor memory or reduce batch sizes",
            "expected_impact": "Prevent OOM errors"
        })
    
    # Skew optimization recommendations
    if metrics.get("skew_ratio", 1) > 5:
        recommendations.append({
            "type": "Data Skew",
            "priority": "Critical",
            "recommendation": "Apply salting or alternative partitioning strategy",
            "expected_impact": "5-20x performance improvement"
        })
    
    return recommendations
```

**Example Usage:**
```python
# Collect all performance metrics
all_metrics = {
    "performance": performance_metrics,
    "resource_usage": cluster_metrics,
    "skew_analysis": skew_analysis,
    "bottlenecks": bottleneck_analysis,
    "memory_leaks": memory_analysis
}

# Generate comprehensive report
performance_report = monitor.generate_performance_report(all_metrics)

# Save report
report_path = f"/reports/spark_performance_{int(time.time())}.json"
with open(report_path, 'w') as f:
    json.dump(performance_report, f, indent=2, default=str)

print(f"Performance report saved to: {report_path}")
```

## Complete Usage Examples

### Example 1: Production Performance Monitoring
```python
# Initialize monitoring
monitor = SparkMonitoringDebugger("ProductionETL")

# Monitor large ETL job
sales_etl_df = spark.read.table("raw_sales_data")

# Step 1: Profile main operations
performance_metrics = monitor.performance_profiling(sales_etl_df, "Sales ETL")

# Step 2: Analyze resource usage
resource_metrics = monitor.monitor_resource_usage()

# Step 3: Check for data skew
skew_analysis = monitor.debug_data_skew(sales_etl_df, "customer_id")

# Step 4: Identify bottlenecks
etl_operations = {
    "data_cleaning": lambda x: x.filter(col("amount") > 0).dropna(),
    "enrichment": lambda x: x.join(customer_dim, "customer_id"),
    "aggregation": lambda x: x.groupBy("region", "month").sum("amount")
}
bottlenecks = monitor.bottleneck_identification(sales_etl_df, etl_operations)

# Step 5: Generate comprehensive report
all_metrics = {
    "performance": performance_metrics,
    "resources": resource_metrics,
    "skew": skew_analysis,
    "bottlenecks": bottlenecks
}
report = monitor.generate_performance_report(all_metrics)
```

### Example 2: Performance Troubleshooting Workflow
```python
# Troubleshoot slow Spark job
problem_df = spark.read.table("problematic_dataset")

# Quick performance assessment
quick_metrics = monitor.performance_profiling(problem_df, "Problem Analysis")

# If performance is poor, drill down:
if quick_metrics["throughput_rows_per_sec"] < 1000:
    # Check for data skew
    skew_results = monitor.debug_data_skew(problem_df, "partition_key")
    
    # Analyze query execution
    query_metrics = monitor.analyze_query_execution(problem_df, "Problem Query")
    
    # Check for memory leaks
    leak_test = {
        "problematic_operation": lambda: problem_df.groupBy("key").sum("value")
    }
    memory_analysis = monitor.memory_leak_detection(leak_test)
    
    # Generate recommendations
    troubleshooting_report = monitor.generate_performance_report({
        "performance": quick_metrics,
        "skew": skew_results,
        "queries": query_metrics,
        "memory": memory_analysis
    })
```

## Monitoring Best Practices Checklist

| Monitoring Area | Action | Frequency | Alert Threshold |
|----------------|--------|-----------|-----------------|
| **Performance Metrics** | Profile key operations | Per job | <1000 rows/sec |
| **Resource Usage** | Monitor cluster utilization | Continuous | >85% memory |
| **Data Skew** | Analyze partition distribution | Weekly | Skew ratio >5 |
| **Memory Leaks** | Test iterative operations | Pre-production | >50MB/iteration |
| **Query Execution** | Review execution plans | Per optimization cycle | >2x baseline time |

This comprehensive documentation covers all monitoring and debugging techniques with practical examples for maintaining high-performance Spark applications in production environments.
