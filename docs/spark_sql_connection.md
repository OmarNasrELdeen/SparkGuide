# Spark JDBC Connection Documentation

## Overview
The `spark_sql_connection.py` module provides comprehensive JDBC connectivity to multiple database systems with advanced partitioning and optimization features.

## Class: SparkJDBCConnector

### Initialization

```python
connector = SparkJDBCConnector(app_name="SparkJDBCETL")
```

#### Spark Configuration Options

| Configuration | Value | Purpose | When to Use |
|---------------|-------|---------|-------------|
| `spark.jars.packages` | Multiple JDBC drivers | Auto-download database drivers | Always for JDBC connections |
| `spark.sql.adaptive.enabled` | `true` | Enable Adaptive Query Execution | Large datasets, complex queries |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Automatically merge small partitions | When many small partitions exist |
| `spark.sql.execution.arrow.pyspark.enabled` | `true` | Enable Arrow-based columnar data transfers | Faster pandas DataFrame conversions |

**Example Usage:**
```python
# Initialize with custom app name
connector = SparkJDBCConnector("MyETLPipeline")

# Access the underlying Spark session
spark = connector.spark
```

### Database Configurations (db_configs)

#### SQL Server Configuration
```python
"sqlserver": {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "port": 1433,
    "url_format": "jdbc:sqlserver://{server}:{port};databaseName={database}"
}
```

#### MySQL Configuration
```python
"mysql": {
    "driver": "com.mysql.cj.jdbc.Driver", 
    "port": 3306,
    "url_format": "jdbc:mysql://{server}:{port}/{database}"
}
```

#### PostgreSQL Configuration
```python
"postgresql": {
    "driver": "org.postgresql.Driver",
    "port": 5432,
    "url_format": "jdbc:postgresql://{server}:{port}/{database}"
}
```

#### Oracle Configuration
```python
"oracle": {
    "driver": "oracle.jdbc.driver.OracleDriver",
    "port": 1521,
    "url_format": "jdbc:oracle:thin:@{server}:{port}:{database}"
}
```

## Method: get_optimized_connection_properties()

### Purpose
Generates database-specific optimized connection properties for maximum performance.

### Parameters
- `db_type`: Database type ("sqlserver", "mysql", "postgresql", "oracle")
- `username`: Database username
- `password`: Database password
- `**kwargs`: Additional custom properties

### SQL Server Optimizations

| Property | Value | Purpose | Performance Impact |
|----------|-------|---------|-------------------|
| `encrypt` | `"true"` | Enable encryption | Security compliance |
| `trustServerCertificate` | `"true"` | Trust server certificate | Faster SSL handshake |
| `loginTimeout` | `"30"` | Connection timeout in seconds | Prevent hanging connections |
| `socketTimeout` | `"0"` | Socket read timeout (0=infinite) | Long-running queries |
| `selectMethod` | `"cursor"` | Use server-side cursors | Memory efficient for large results |
| `sendStringParametersAsUnicode` | `"false"` | Reduce network traffic | 2x faster string parameters |
| `prepareThreshold` | `"3"` | Prepare statements after 3 uses | Faster repeated queries |
| `packetSize` | `"8192"` | Network packet size in bytes | Optimal for most networks |

**Example Usage:**
```python
# Get optimized SQL Server properties
properties = connector.get_optimized_connection_properties(
    db_type="sqlserver",
    username="myuser",
    password="mypass",
    # Custom properties
    applicationName="MyETLApp",
    workstationID="ETL_SERVER_01"
)
```

### MySQL Optimizations

| Property | Value | Purpose | Performance Impact |
|----------|-------|---------|-------------------|
| `useSSL` | `"false"` | Disable SSL for local connections | Faster local connections |
| `allowPublicKeyRetrieval` | `"true"` | Allow public key retrieval | Required for some MySQL versions |
| `useUnicode` | `"true"` | Enable Unicode support | Proper character encoding |
| `characterEncoding` | `"utf8"` | Set character encoding | Consistent text handling |
| `autoReconnect` | `"true"` | Auto-reconnect on connection loss | Better fault tolerance |
| `cachePrepStmts` | `"true"` | Cache prepared statements | 5-10x faster repeated queries |
| `prepStmtCacheSize` | `"250"` | Number of cached statements | Memory vs performance trade-off |
| `prepStmtCacheSqlLimit` | `"2048"` | Max SQL length for caching | Cache only reasonable-sized queries |
| `useServerPrepStmts` | `"true"` | Use server-side prepared statements | Better performance for complex queries |

**Example Usage:**
```python
# Get optimized MySQL properties
properties = connector.get_optimized_connection_properties(
    db_type="mysql",
    username="myuser", 
    password="mypass",
    # Custom MySQL properties
    connectTimeout="60000",
    socketTimeout="300000"
)
```

### PostgreSQL Optimizations

| Property | Value | Purpose | Performance Impact |
|----------|-------|---------|-------------------|
| `ssl` | `"false"` | Disable SSL for local connections | Faster local connections |
| `prepareThreshold` | `"5"` | Prepare after 5 uses | Optimized for PostgreSQL |
| `preparedStatementCacheQueries` | `"256"` | Number of cached prepared statements | Memory vs performance |
| `preparedStatementCacheSizeMiB` | `"5"` | Cache size in MB | Memory allocation for cache |
| `defaultRowFetchSize` | `"10000"` | Rows fetched per round trip | Balance memory vs network trips |

**Example Usage:**
```python
# Get optimized PostgreSQL properties
properties = connector.get_optimized_connection_properties(
    db_type="postgresql",
    username="myuser",
    password="mypass",
    # Custom PostgreSQL properties
    applicationName="SparkETL",
    currentSchema="analytics"
)
```

### Oracle Optimizations

| Property | Value | Purpose | Performance Impact |
|----------|-------|---------|-------------------|
| `oracle.jdbc.ReadTimeout` | `"0"` | Socket read timeout (0=infinite) | Long-running queries |
| `oracle.net.CONNECT_TIMEOUT` | `"10000"` | Connection timeout in ms | Faster failure detection |
| `oracle.jdbc.defaultRowPrefetch` | `"20"` | Rows prefetched per round trip | Reduced network round trips |
| `useFetchSizeWithLongColumn` | `"true"` | Use fetch size with LONG columns | Better memory management |

**Example Usage:**
```python
# Get optimized Oracle properties
properties = connector.get_optimized_connection_properties(
    db_type="oracle",
    username="myuser",
    password="mypass",
    # Custom Oracle properties
    "oracle.jdbc.J2EE13Compliant": "true",
    "oracle.jdbc.implicitStatementCacheSize": "25"
)
```

## Method: extract_with_partitioning()

### Purpose
Extract data with numeric partitioning for parallel processing using lower and upper bounds.

### Parameters
- `db_type`: Database type
- `server`: Database server hostname/IP
- `database`: Database name
- `username`: Database username
- `password`: Database password
- `table_name`: Table to extract from
- `partition_column`: Column to partition on (must be numeric)
- `lower_bound`: Lower bound value for partitioning
- `upper_bound`: Upper bound value for partitioning
- `num_partitions`: Number of parallel partitions
- `port`: Custom port (optional)
- `**connection_options`: Additional connection properties

### How Partitioning Works
Spark divides the value range `[lower_bound, upper_bound]` into `num_partitions` equal ranges:
- Partition 1: `partition_column >= lower_bound AND partition_column < (lower_bound + stride)`
- Partition 2: `partition_column >= (lower_bound + stride) AND partition_column < (lower_bound + 2*stride)`
- ...
- Last Partition: `partition_column >= (upper_bound - stride) AND partition_column <= upper_bound`

Where `stride = (upper_bound - lower_bound) / num_partitions`

### When to Use
- ✅ Large tables (>1M rows)
- ✅ Numeric partition column (int, bigint, decimal)
- ✅ Evenly distributed data
- ✅ Need parallel processing

### When NOT to Use
- ❌ Small tables (<100K rows)
- ❌ Highly skewed data distribution
- ❌ String/date partition columns (use custom predicates instead)

**Example Usage:**
```python
# Extract large sales table with ID partitioning
df = connector.extract_with_partitioning(
    db_type="sqlserver",
    server="sql-server.company.com",
    database="sales_db",
    username="etl_user",
    password="secure_password",
    table_name="sales_transactions",
    partition_column="transaction_id",
    lower_bound=1,                    # Minimum ID value
    upper_bound=10000000,            # Maximum ID value  
    num_partitions=20,               # 20 parallel tasks
    port=1433                        # Custom port
)

# This creates 20 partitions:
# Partition 1: transaction_id >= 1 AND transaction_id < 500000
# Partition 2: transaction_id >= 500000 AND transaction_id < 1000000
# ...
# Partition 20: transaction_id >= 9500000 AND transaction_id <= 10000000
```

### Performance Considerations

| Factor | Recommendation | Reason |
|--------|---------------|---------|
| **Number of Partitions** | 2-4x number of CPU cores | Maximize parallelism without over-partitioning |
| **Partition Size** | 100K-1M rows per partition | Balance memory usage and task overhead |
| **Network Bandwidth** | Consider available bandwidth | More partitions = more concurrent connections |
| **Database Load** | Monitor database CPU/IO | Too many partitions can overwhelm source DB |

**Optimal Partitioning Example:**
```python
# For 8-core cluster with 10M row table
df = connector.extract_with_partitioning(
    # ...connection parameters...
    partition_column="order_id",
    lower_bound=1,
    upper_bound=10000000,
    num_partitions=32              # 4x cores = 32 partitions
    # Each partition: ~312K rows (10M/32)
)
```

## Method: extract_with_custom_partitioning()

### Purpose
Extract data using custom SQL predicates for complex partitioning logic that can't be expressed with simple bounds.

### Parameters
- `db_type`: Database type
- `server`: Database server
- `database`: Database name  
- `username`: Database username
- `password`: Database password
- `table_name`: Table to extract from
- `partition_predicates`: List of SQL WHERE clauses
- `port`: Custom port (optional)
- `**connection_options`: Additional connection properties

### When to Use
- ✅ String-based partitioning (regions, categories)
- ✅ Date-based partitioning with complex logic
- ✅ Business rule partitioning
- ✅ Skewed data distribution
- ✅ Multiple column partitioning

**Example Usage - Regional Partitioning:**
```python
# Partition by geographical regions
regional_predicates = [
    "region_code IN ('US-WEST', 'US-CENTRAL')",
    "region_code IN ('US-EAST', 'US-SOUTH')", 
    "region_code IN ('CA-ON', 'CA-BC')",
    "region_code IN ('EU-UK', 'EU-DE')",
    "region_code NOT IN ('US-WEST', 'US-CENTRAL', 'US-EAST', 'US-SOUTH', 'CA-ON', 'CA-BC', 'EU-UK', 'EU-DE')"
]

df = connector.extract_with_custom_partitioning(
    db_type="postgresql",
    server="analytics-db.company.com",
    database="customer_data",
    username="etl_user",
    password="password",
    table_name="customer_orders",
    partition_predicates=regional_predicates
)
```

**Example Usage - Status-Based Partitioning:**
```python
# Partition by order status for balanced processing
status_predicates = [
    "order_status = 'PENDING'",
    "order_status = 'PROCESSING'", 
    "order_status = 'SHIPPED'",
    "order_status = 'DELIVERED'",
    "order_status IN ('CANCELLED', 'RETURNED', 'REFUNDED')"
]

df = connector.extract_with_custom_partitioning(
    db_type="mysql",
    server="localhost",
    database="orders_db", 
    username="user",
    password="pass",
    table_name="orders",
    partition_predicates=status_predicates
)
```

**Example Usage - Complex Business Logic:**
```python
# Partition by customer tier and activity
business_predicates = [
    "customer_tier = 'PREMIUM' AND last_order_date >= '2024-01-01'",
    "customer_tier = 'GOLD' AND last_order_date >= '2024-01-01'",
    "customer_tier = 'SILVER' AND total_orders > 10",
    "customer_tier = 'BRONZE' OR total_orders <= 10",
    "customer_tier IS NULL OR last_order_date < '2024-01-01'"
]

df = connector.extract_with_custom_partitioning(
    db_type="sqlserver",
    server="crm-db.company.com", 
    database="customer_db",
    username="analytics_user",
    password="secure_pass",
    table_name="customers",
    partition_predicates=business_predicates
)
```

### Performance Tips for Custom Predicates

| Tip | Example | Benefit |
|-----|---------|---------|
| **Use Indexed Columns** | `"customer_id BETWEEN 1 AND 1000"` | Faster predicate evaluation |
| **Balance Partition Sizes** | Ensure similar row counts per predicate | Even processing load |
| **Avoid Complex Joins** | Pre-join data or use simple filters | Reduced query complexity |
| **Test Predicates** | Validate each predicate returns data | Avoid empty partitions |

## Method: extract_with_date_partitioning()

### Purpose
Extract data with automatic date-based partitioning strategies (daily, weekly, monthly).

### Parameters
- `db_type`: Database type
- `server`: Database server
- `database`: Database name
- `username`: Database username  
- `password`: Database password
- `table_name`: Table to extract from
- `date_column`: Date column to partition on
- `start_date`: Start date (YYYY-MM-DD format)
- `end_date`: End date (YYYY-MM-DD format)
- `partition_strategy`: "daily", "weekly", or "monthly"
- `port`: Custom port (optional)
- `**connection_options`: Additional connection properties

### Partitioning Strategies

#### Daily Partitioning
- Creates one partition per day
- Use for: High-volume time-series data, daily reporting

```python
df = connector.extract_with_date_partitioning(
    db_type="postgresql",
    server="timeseries-db.company.com",
    database="events",
    username="etl_user", 
    password="password",
    table_name="user_events",
    date_column="event_timestamp",
    start_date="2024-01-01",
    end_date="2024-01-31",           # 31 partitions (one per day)
    partition_strategy="daily"
)

# Generated predicates:
# event_timestamp >= '2024-01-01' AND event_timestamp < '2024-01-02'
# event_timestamp >= '2024-01-02' AND event_timestamp < '2024-01-03'
# ...
# event_timestamp >= '2024-01-31' AND event_timestamp < '2024-02-01'
```

#### Weekly Partitioning  
- Creates one partition per week
- Use for: Medium-volume data, weekly analysis

```python
df = connector.extract_with_date_partitioning(
    db_type="mysql",
    server="analytics-db.company.com",
    database="sales",
    username="analyst",
    password="password", 
    table_name="weekly_sales",
    date_column="sale_date",
    start_date="2024-01-01",
    end_date="2024-12-31",           # ~52 partitions (one per week)
    partition_strategy="weekly"
)

# Generated predicates (7-day intervals):
# sale_date >= '2024-01-01' AND sale_date < '2024-01-08'
# sale_date >= '2024-01-08' AND sale_date < '2024-01-15'
# ...
```

#### Monthly Partitioning
- Creates one partition per month  
- Use for: Historical data, monthly reporting, large date ranges

```python
df = connector.extract_with_date_partitioning(
    db_type="sqlserver",
    server="warehouse-db.company.com",
    database="historical_data",
    username="etl_service",
    password="secure_password",
    table_name="transactions",
    date_column="transaction_date", 
    start_date="2020-01-01",
    end_date="2024-12-31",           # 60 partitions (5 years × 12 months)
    partition_strategy="monthly"
)

# Generated predicates (month boundaries):
# transaction_date >= '2020-01-01' AND transaction_date < '2020-02-01'
# transaction_date >= '2020-02-01' AND transaction_date < '2020-03-01'
# ...
# transaction_date >= '2024-12-01' AND transaction_date < '2025-01-01'
```

### Strategy Selection Guide

| Data Volume | Time Range | Recommended Strategy | Example Use Case |
|-------------|------------|---------------------|------------------|
| **High** (>1M rows/day) | Days to months | Daily | Real-time analytics, IoT data |
| **Medium** (100K-1M rows/day) | Weeks to quarters | Weekly | Sales data, user activity |
| **Low** (<100K rows/day) | Months to years | Monthly | Historical analysis, reporting |

### Performance Considerations

| Factor | Daily | Weekly | Monthly |
|--------|-------|--------|---------|
| **Partition Count** | High (365/year) | Medium (52/year) | Low (12/year) |
| **Partition Size** | Small | Medium | Large |
| **Query Performance** | Fastest for date ranges | Balanced | Slower for small date ranges |
| **Memory Usage** | Low per partition | Medium | High per partition |

## Method: optimize_connection_for_read_performance()

### Purpose
Provides optimized connection settings recommendations based on estimated data size and database type.

### Parameters
- `db_type`: Database type
- `table_name`: Table name (for logging)
- `estimated_rows`: Estimated number of rows

### Calculation Logic

#### Partition Recommendations
- **Target Partition Size**: 100,000 rows per partition
- **Minimum Partitions**: 1
- **Maximum Partitions**: 200
- **Formula**: `min(200, max(1, estimated_rows // 100000))`

#### Fetch Size Recommendations  
- **Calculation**: `min(10000, max(1000, estimated_rows // recommended_partitions // 10))`
- **Minimum**: 1,000 rows per fetch
- **Maximum**: 10,000 rows per fetch

**Example Usage:**
```python
# Get recommendations for large table
recommendations = connector.optimize_connection_for_read_performance(
    db_type="sqlserver",
    table_name="large_sales_table", 
    estimated_rows=5000000           # 5 million rows
)

# Output:
# Estimated rows: 5,000,000
# Recommended partitions: 50
# Recommended fetch size: 10,000
# Database-specific optimizations:
#   packetSize: 32768
#   responseBuffering: adaptive
#   selectMethod: cursor

# Use recommendations in extraction
df = connector.extract_with_partitioning(
    # ...connection parameters...
    num_partitions=recommendations["num_partitions"],  # 50
    **{k: v for k, v in recommendations.items() 
       if k not in ["num_partitions", "fetchsize"]}
)
```

### Database-Specific Recommendations

#### SQL Server
| Estimated Rows | packetSize | responseBuffering | selectMethod |
|---------------|------------|-------------------|--------------|
| > 1,000,000 | 32768 | adaptive | cursor |
| ≤ 1,000,000 | 8192 | full | cursor |
| ≤ 100,000 | 8192 | full | direct |

#### MySQL  
| Estimated Rows | useCursorFetch | defaultFetchSize |
|---------------|----------------|------------------|
| > 100,000 | true | calculated |
| ≤ 100,000 | false | calculated |

#### PostgreSQL
| Estimated Rows | prepareThreshold | defaultRowFetchSize |
|---------------|------------------|-------------------|
| > 50,000 | 3 | calculated |
| ≤ 50,000 | 0 | calculated |

#### Oracle
| Property | Value | Purpose |
|----------|-------|---------|
| `oracle.jdbc.defaultRowPrefetch` | `min(50, fetchsize // 100)` | Optimized prefetch |
| `oracle.jdbc.useFetchSizeWithLongColumn` | `true` | Handle LONG columns |

## Method: write_to_database()

### Purpose
Write DataFrame back to database with optimized settings for bulk inserts.

### Parameters
- `df`: DataFrame to write
- `db_type`: Target database type
- `server`: Database server
- `database`: Database name
- `username`: Database username
- `password`: Database password
- `table_name`: Target table name
- `mode`: Write mode ("append", "overwrite", "ignore", "error")
- `port`: Custom port (optional)
- `batch_size`: Number of rows per batch (default: 10,000)
- `**connection_options`: Additional connection properties

### Write Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `"append"` | Add rows to existing table | Incremental loads |
| `"overwrite"` | Replace entire table | Full refreshes |
| `"ignore"` | Skip if table exists | Safe operations |
| `"error"` | Fail if table exists | Prevent accidental overwrites |

### Write Optimizations

#### Automatic Optimizations
- **Partition Optimization**: Repartitions DataFrame for optimal write parallelism
- **Batch Size**: Configurable batch size for bulk inserts
- **Isolation Level**: Uses `READ_UNCOMMITTED` for faster writes
- **Connection Pooling**: Reuses connections across write operations

#### SQL Server Write Optimizations
```python
{
    "bulkCopyOptions": "FIRE_TRIGGERS,CHECK_CONSTRAINTS",
    "bulkCopyBatchSize": str(batch_size),
    "bulkCopyTimeout": "0"
}
```

#### MySQL Write Optimizations
```python
{
    "rewriteBatchedStatements": "true",    # Batch multiple INSERTs
    "useServerPrepStmts": "false"          # Better for bulk inserts
}
```

**Example Usage:**
```python
# Write processed data back to SQL Server
connector.write_to_database(
    df=processed_df,
    db_type="sqlserver",
    server="data-warehouse.company.com",
    database="analytics",
    username="etl_writer",
    password="secure_password",
    table_name="processed_sales",
    mode="append",                    # Add to existing data
    batch_size=20000,                # Large batches for performance
    # Custom optimizations
    trustServerCertificate="true",
    applicationName="SparkETL"
)

# Output:
# Successfully wrote 2,543,891 rows to processed_sales
# Write mode: append, Batch size: 20000
```

### Performance Tuning

#### Optimal Partition Count for Writing
```python
# Automatic calculation
row_count = df.count()
optimal_partitions = max(1, min(20, row_count // 50000))

# Manual override for specific scenarios
if row_count > 5000000:
    optimal_partitions = 40  # More parallelism for very large datasets
elif row_count < 100000:
    optimal_partitions = 1   # Single partition for small datasets
```

#### Batch Size Guidelines

| Row Count | Recommended Batch Size | Reasoning |
|-----------|----------------------|-----------|
| < 100K | 5,000 | Reduce overhead |
| 100K - 1M | 10,000 | Default balance |
| 1M - 10M | 20,000 | Larger batches for efficiency |
| > 10M | 50,000 | Maximum throughput |

## Complete Usage Examples

### Example 1: Large Table ETL Pipeline
```python
# Initialize connector
connector = SparkJDBCConnector("SalesETLPipeline")

# Get optimization recommendations
recommendations = connector.optimize_connection_for_read_performance(
    db_type="sqlserver",
    table_name="sales_transactions",
    estimated_rows=10000000
)

# Extract with optimized settings
source_df = connector.extract_with_partitioning(
    db_type="sqlserver",
    server="source-db.company.com",
    database="sales_oltp",
    username="etl_reader", 
    password="read_password",
    table_name="sales_transactions",
    partition_column="transaction_id",
    lower_bound=1,
    upper_bound=10000000,
    num_partitions=recommendations["num_partitions"],
    **{k: v for k, v in recommendations.items() 
       if k not in ["num_partitions", "fetchsize"]}
)

# Process data (example transformation)
processed_df = source_df \
    .filter(col("amount") > 0) \
    .withColumn("processed_date", current_date()) \
    .groupBy("customer_id", "product_category") \
    .agg(sum("amount").alias("total_amount"))

# Write to data warehouse
connector.write_to_database(
    df=processed_df,
    db_type="postgresql", 
    server="warehouse-db.company.com",
    database="analytics_dw",
    username="etl_writer",
    password="write_password", 
    table_name="customer_product_summary",
    mode="overwrite",
    batch_size=25000
)
```

### Example 2: Multi-Database ETL with Date Partitioning
```python
# Extract from multiple sources with date partitioning
orders_df = connector.extract_with_date_partitioning(
    db_type="mysql",
    server="orders-db.company.com",
    database="ecommerce",
    username="etl_user",
    password="password",
    table_name="orders",
    date_column="order_date",
    start_date="2024-01-01", 
    end_date="2024-12-31",
    partition_strategy="monthly"
)

customers_df = connector.extract_with_custom_partitioning(
    db_type="postgresql",
    server="crm-db.company.com", 
    database="customer_data",
    username="etl_user",
    password="password",
    table_name="customers",
    partition_predicates=[
        "customer_segment = 'PREMIUM'",
        "customer_segment = 'STANDARD'", 
        "customer_segment = 'BASIC'",
        "customer_segment IS NULL"
    ]
)

# Join and aggregate
summary_df = orders_df \
    .join(customers_df, "customer_id") \
    .groupBy("customer_segment", "order_month") \
    .agg(
        sum("order_amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_customers")
    )

# Write final results
connector.write_to_database(
    df=summary_df,
    db_type="sqlserver",
    server="reporting-db.company.com",
    database="business_intelligence", 
    username="bi_writer",
    password="bi_password",
    table_name="monthly_customer_summary",
    mode="overwrite",
    batch_size=10000
)
```

## Troubleshooting Guide

### Common Issues and Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| **Driver Not Found** | `ClassNotFoundException` | Add driver to `spark.jars.packages` |
| **Connection Timeout** | Hangs during connection | Increase `loginTimeout` or check network |
| **Memory Errors** | `OutOfMemoryError` | Reduce `num_partitions` or `fetchsize` |
| **Slow Performance** | Long query execution | Optimize partitioning or use indexed columns |
| **Empty Partitions** | Uneven data distribution | Use custom predicates instead of bounds |

### Performance Monitoring
```python
# Monitor partition distribution
partition_sizes = df.rdd.glom().map(len).collect()
print(f"Partition sizes: min={min(partition_sizes)}, max={max(partition_sizes)}")

# Check for skewed partitions
avg_size = sum(partition_sizes) / len(partition_sizes)
max_size = max(partition_sizes)
skew_ratio = max_size / avg_size
if skew_ratio > 2:
    print(f"WARNING: High skew detected (ratio: {skew_ratio:.2f})")
```

This comprehensive documentation covers all configurations, methods, and options in the JDBC connection module with practical examples and performance considerations.
