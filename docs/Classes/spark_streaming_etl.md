# Spark Streaming ETL Documentation

## Overview
The `spark_streaming_etl.py` module demonstrates streaming ETL patterns and real-time data processing optimization using Spark Structured Streaming for continuous data pipelines.

## Class: SparkStreamingETL

### Initialization

```python
streaming_etl = SparkStreamingETL(app_name="SparkStreamingETL")
```

#### Streaming-Specific Spark Configuration Options

| Configuration | Value | Purpose | Impact |
|---------------|-------|---------|---------|
| `spark.sql.adaptive.enabled` | `"true"` | Enable AQE for streaming | Better micro-batch optimization |
| `spark.sql.streaming.metricsEnabled` | `"true"` | Enable streaming metrics | Monitoring and debugging |
| `spark.sql.streaming.ui.enabled` | `"true"` | Enable streaming UI | Visual monitoring |
| `spark.sql.streaming.checkpointLocation.deleteOnExit` | `"true"` | Auto-cleanup checkpoints | Testing convenience |

## Method: kafka_streaming_source()

### Purpose
Sets up optimized Kafka streaming source with parsing and performance tuning for high-throughput data ingestion.

### Parameters
- `kafka_servers`: Kafka bootstrap servers
- `topics`: Topics to subscribe to
- `checkpoint_location`: Location for checkpoint data

### Kafka Source Optimization Options

| Option | Value | Purpose | Performance Impact |
|--------|-------|---------|-------------------|
| `kafka.bootstrap.servers` | Server list | Kafka cluster connection | Redundancy and load balancing |
| `subscribe` | Topic names | Topics to consume | Parallel processing per topic |
| `startingOffsets` | `"latest"` | Where to start reading | Avoid processing old data |
| `maxOffsetsPerTrigger` | `100000` | Records per micro-batch | Control batch size |
| `kafka.consumer.session.timeout.ms` | `30000` | Consumer session timeout | Handle network issues |
| `kafka.consumer.heartbeat.interval.ms` | `10000` | Heartbeat frequency | Consumer liveness |
| `failOnDataLoss` | `"false"` | Handle data loss gracefully | Production resilience |

### Message Parsing Strategy
```python
# Kafka message structure
kafka_schema = StructType([
    StructField("id", IntegerType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("transaction_time", StringType()),
    StructField("category", StringType())
])

# Parse JSON messages from Kafka
parsed_df = kafka_df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
               "topic", "partition", "offset", "timestamp") \
    .withColumn("parsed_value", from_json(col("value"), kafka_schema)) \
    .select("key", "topic", "partition", "offset", "timestamp", "parsed_value.*")
```

**Example Usage:**
```python
# Setup Kafka streaming source
kafka_servers = "kafka1:9092,kafka2:9092,kafka3:9092"
topics = "transactions,orders,events"
checkpoint_path = "/checkpoints/kafka_source"

streaming_df = streaming_etl.kafka_streaming_source(
    kafka_servers=kafka_servers,
    topics=topics,
    checkpoint_location=checkpoint_path
)

# Result: Parsed streaming DataFrame with transaction data
# Columns: key, topic, partition, offset, timestamp, id, customer_id, amount, transaction_time, category
```

### Kafka Performance Tuning

| Parameter | Small Load | Medium Load | High Load | Purpose |
|-----------|------------|-------------|-----------|---------|
| `maxOffsetsPerTrigger` | 10,000 | 100,000 | 1,000,000 | Batch size control |
| `session.timeout.ms` | 10,000 | 30,000 | 60,000 | Network resilience |
| `heartbeat.interval.ms` | 3,000 | 10,000 | 20,000 | Consumer health |
| `fetch.max.wait.ms` | 500 | 500 | 100 | Latency vs throughput |

## Method: streaming_transformations()

### Purpose
Applies real-time transformations including watermarking, windowed aggregations, and event-time processing.

### Watermarking for Late Data Handling
```python
# Handle late-arriving data with watermarking
watermarked_df = streaming_df \
    .withWatermark("timestamp", "10 minutes")  # Allow 10 minutes of late data

# Watermarking benefits:
# - Enables state cleanup for aggregations
# - Handles out-of-order events
# - Prevents unbounded state growth
```

### Windowed Aggregations
```python
# Time-based windowed aggregations
windowed_aggregations = watermarked_df \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),  # 5-min window, 1-min slide
        col("category")
    ) \
    .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count"),
        avg("amount").alias("avg_amount"),
        countDistinct("customer_id").alias("unique_customers")
    )

# Window types:
# - Tumbling: window("timestamp", "5 minutes") - non-overlapping
# - Sliding: window("timestamp", "5 minutes", "1 minute") - overlapping
# - Session: sessionWindow("timestamp", "5 minutes") - gap-based
```

### Real-time Alerting Patterns
```python
# High-value transaction alerts
high_value_alerts = watermarked_df \
    .filter(col("amount") > 10000) \
    .withColumn("alert_type", lit("HIGH_VALUE")) \
    .withColumn("alert_timestamp", current_timestamp()) \
    .withColumn("risk_score", col("amount") / 1000)

# Fraud detection with complex logic
fraud_detection = watermarked_df \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("customer_id")
    ) \
    .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        countDistinct("category").alias("category_diversity")
    ) \
    .withColumn("fraud_score",
               col("transaction_count") * 0.3 + 
               col("total_amount") / 1000 * 0.4 +
               col("category_diversity") * 0.3) \
    .filter(col("fraud_score") > 20)
```

**Example Usage:**
```python
# Apply streaming transformations
windowed_agg, high_value_alerts, fraud_alerts = streaming_etl.streaming_transformations(streaming_df)

# Results provide:
# - windowed_agg: 5-minute rolling aggregations by category
# - high_value_alerts: Real-time alerts for large transactions
# - fraud_alerts: Fraud detection based on behavioral patterns
```

### Streaming Transformation Patterns

| Pattern | Use Case | Latency | Complexity |
|---------|----------|---------|------------|
| **Simple Filtering** | Data quality, routing | Milliseconds | Low |
| **Windowed Aggregations** | Metrics, KPIs | Seconds | Medium |
| **Complex Event Processing** | Fraud detection | Seconds to minutes | High |
| **ML Inference** | Real-time scoring | Milliseconds to seconds | Medium |

## Method: streaming_join_operations()

### Purpose
Demonstrates different types of streaming joins for data enrichment and correlation.

### Stream-Static Joins (Dimension Lookups)
```python
# Enrich streaming data with static dimension tables
enriched_stream = streaming_df \
    .join(broadcast(static_dimension_df), "category") \
    .select("*", "category_description", "category_multiplier")

# Benefits:
# - Fast lookups (broadcast join)
# - Real-time enrichment
# - No additional latency
```

### Stream-Stream Joins
```python
# Join two streaming DataFrames
stream1_watermarked = stream1.withWatermark("timestamp", "5 minutes")
stream2_watermarked = stream2.withWatermark("timestamp", "5 minutes")

joined_streams = stream1_watermarked.join(
    stream2_watermarked,
    expr("""
        customer_id = customer_id AND 
        timestamp >= timestamp - interval 2 minutes AND
        timestamp <= timestamp + interval 2 minutes
    """)
)

# Considerations:
# - Both streams must have watermarks
# - Join conditions must include time bounds
# - State cleanup based on watermarks
```

### Streaming Join Types

| Join Type | Use Case | Latency | State Requirements |
|-----------|----------|---------|-------------------|
| **Stream-Static** | Dimension lookup | Low | Minimal (broadcast) |
| **Stream-Stream (Inner)** | Event correlation | Medium | Bounded by watermark |
| **Stream-Stream (Outer)** | Missing event detection | High | Large state |

**Example Usage:**
```python
# Static dimension table
category_dim = spark.read.table("category_dimensions")

# Apply streaming joins
enriched_stream, correlated_events = streaming_etl.streaming_join_operations(
    streaming_df, 
    category_dim
)

# Results:
# - enriched_stream: Transactions with category details
# - correlated_events: Related transactions within time windows
```

## Method: streaming_sinks_optimization()

### Purpose
Optimizes different streaming sink types for various output requirements and performance needs.

### File Sink Optimization
```python
# Optimized file sink with partitioning
file_sink_query = streaming_df \
    .writeStream \
    .format("parquet") \
    .option("path", "/data/streaming_output") \
    .option("checkpointLocation", "/checkpoints/file_sink") \
    .partitionBy("date", "category") \
    .trigger(processingTime="30 seconds") \
    .outputMode("append") \
    .start()

# File sink benefits:
# - Partitioned for query performance
# - Configurable trigger intervals
# - ACID guarantees with Delta Lake
```

### Kafka Sink for Downstream Processing
```python
# Stream processing results back to Kafka
kafka_sink_query = streaming_df \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092") \
    .option("topic", "processed_events") \
    .option("checkpointLocation", "/checkpoints/kafka_sink") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()
```

### Database Sink for Real-time Updates
```python
# Custom database sink using foreachBatch
def write_to_database(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/streaming_db") \
        .option("dbtable", "real_time_metrics") \
        .option("user", "stream_user") \
        .option("password", "password") \
        .mode("append") \
        .save()

database_sink_query = streaming_df \
    .writeStream \
    .foreachBatch(write_to_database) \
    .trigger(processingTime="1 minute") \
    .start()
```

### Console Sink for Development
```python
# Debug and development sink
console_query = streaming_df \
    .writeStream \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()
```

**Example Usage:**
```python
# Setup multiple streaming sinks
output_path = "/data/streaming_results"
checkpoint_path = "/checkpoints/multi_sink"

sink_queries = streaming_etl.streaming_sinks_optimization(
    streaming_df=processed_df,
    output_path=output_path,
    checkpoint_path=checkpoint_path
)

# Monitor all queries
for query_name, query in sink_queries.items():
    print(f"{query_name}: {query.status}")
```

### Sink Performance Comparison

| Sink Type | Throughput | Latency | Use Case |
|-----------|------------|---------|----------|
| **File (Parquet)** | High | Medium | Analytics, archival |
| **Kafka** | Very High | Low | Stream processing chains |
| **Database** | Medium | Medium | Real-time dashboards |
| **Console** | Low | Low | Development, debugging |

## Method: handle_late_data_and_errors()

### Purpose
Handles late-arriving data and implements error handling strategies for robust streaming applications.

### Late Data Handling with Watermarks
```python
# Configure watermarking for late data tolerance
watermarked_df = streaming_df \
    .withWatermark("event_time", "15 minutes")  # 15-minute grace period

# Separate processing for on-time vs late data
current_window = window(col("event_time"), "5 minutes")

on_time_processing = watermarked_df \
    .filter(col("event_time") >= current_timestamp() - expr("INTERVAL 15 MINUTES")) \
    .groupBy(current_window, "category") \
    .agg(sum("amount").alias("on_time_amount"))

# Late data handling strategy
late_data_processing = watermarked_df \
    .filter(col("event_time") < current_timestamp() - expr("INTERVAL 15 MINUTES")) \
    .withColumn("late_data_flag", lit(true)) \
    .groupBy(current_window, "category") \
    .agg(sum("amount").alias("late_amount"))
```

### Error Handling Patterns
```python
# Dead letter queue for failed records
def create_error_handling_stream(df):
    return df \
        .withColumn("is_valid", 
                   col("amount").isNotNull() & 
                   (col("amount") > 0) & 
                   col("customer_id").isNotNull()) \
        .withColumn("error_reason",
                   when(col("amount").isNull(), "NULL_AMOUNT")
                   .when(col("amount") <= 0, "INVALID_AMOUNT")
                   .when(col("customer_id").isNull(), "NULL_CUSTOMER")
                   .otherwise(null()))

# Split valid and invalid records
valid_records = df.filter(col("is_valid") == True)
invalid_records = df.filter(col("is_valid") == False)

# Process valid records normally
# Send invalid records to dead letter queue
```

## Method: streaming_performance_optimization()

### Purpose
Optimizes streaming performance through trigger configuration, resource allocation, and checkpoint management.

### Trigger Optimization Strategies
```python
# Different trigger types for different scenarios
triggers = {
    "processing_time": Trigger.ProcessingTime("30 seconds"),  # Regular intervals
    "once": Trigger.Once(),                                   # Single batch (testing)
    "available_now": Trigger.AvailableNow(),                 # Process all available data
    "continuous": Trigger.Continuous("1 second")             # Experimental low-latency
}

# Choose trigger based on requirements:
# - ProcessingTime: Predictable resource usage
# - AvailableNow: Catch up processing
# - Continuous: Ultra-low latency (experimental)
```

### Checkpoint Optimization
```python
# Optimize checkpoint configuration
checkpoint_config = {
    "checkpointLocation": "/optimized/checkpoints",
    "cleanSource": "delete",              # Clean up source files
    "maxFilesPerTrigger": 1000,          # Limit files per batch
    "latestFirst": True,                 # Process newest files first
    "maxBytesPerTrigger": "1g"          # Limit data per trigger
}
```

### Resource Allocation for Streaming
```python
# Streaming-specific resource configuration
streaming_config = {
    "spark.streaming.backpressure.enabled": "true",
    "spark.streaming.receiver.maxRate": "10000",
    "spark.streaming.kafka.maxRatePerPartition": "1000",
    "spark.streaming.stopGracefullyOnShutdown": "true",
    "spark.sql.streaming.minBatchesToRetain": "10"
}
```

## Method: monitoring_streaming_jobs()

### Purpose
Monitors streaming job performance, health, and provides alerting for issues.

### Query Progress Monitoring
```python
def monitor_streaming_query(query):
    """Monitor streaming query progress and performance"""
    
    while query.isActive:
        progress = query.lastProgress
        
        if progress:
            metrics = {
                "batch_id": progress.get("batchId", 0),
                "input_rate": progress.get("inputRowsPerSecond", 0),
                "processing_rate": progress.get("processedRowsPerSecond", 0),
                "batch_duration": progress.get("batchDuration", 0),
                "trigger_execution": progress.get("triggerExecution", {}),
                "state_operators": progress.get("stateOperators", [])
            }
            
            # Performance analysis
            if metrics["input_rate"] > metrics["processing_rate"] * 1.2:
                print("WARNING: Input rate exceeding processing capacity!")
            
            # State size monitoring
            for state_op in metrics["state_operators"]:
                state_size = state_op.get("numRowsTotal", 0)
                if state_size > 1000000:  # 1M rows threshold
                    print(f"WARNING: Large state size: {state_size}")
        
        time.sleep(10)
```

### Streaming Metrics Collection
```python
def collect_streaming_metrics(query):
    """Collect comprehensive streaming metrics"""
    
    recent_progress = query.recentProgress
    
    metrics = {
        "query_id": query.id,
        "query_name": query.name,
        "status": query.status,
        "batch_metrics": [],
        "performance_trends": {}
    }
    
    # Analyze recent batches
    input_rates = []
    processing_rates = []
    
    for progress in recent_progress:
        batch_metrics = {
            "batch_id": progress.get("batchId"),
            "timestamp": progress.get("timestamp"),
            "input_rows": progress.get("inputRowsPerSecond", 0),
            "processed_rows": progress.get("processedRowsPerSecond", 0),
            "batch_duration": progress.get("batchDuration", 0)
        }
        metrics["batch_metrics"].append(batch_metrics)
        
        input_rates.append(batch_metrics["input_rows"])
        processing_rates.append(batch_metrics["processed_rows"])
    
    # Calculate trends
    if input_rates and processing_rates:
        metrics["performance_trends"] = {
            "avg_input_rate": sum(input_rates) / len(input_rates),
            "avg_processing_rate": sum(processing_rates) / len(processing_rates),
            "throughput_ratio": (sum(processing_rates) / len(processing_rates)) / 
                               (sum(input_rates) / len(input_rates)) if sum(input_rates) > 0 else 0
        }
    
    return metrics
```

## Complete Usage Examples

### Example 1: Real-time Analytics Pipeline
```python
# Initialize streaming ETL
streaming_etl = SparkStreamingETL("RealTimeAnalytics")

# Step 1: Setup Kafka source
kafka_stream = streaming_etl.kafka_streaming_source(
    kafka_servers="kafka1:9092,kafka2:9092",
    topics="transactions,events",
    checkpoint_location="/checkpoints/kafka_source"
)

# Step 2: Apply transformations
windowed_metrics, alerts, fraud_detection = streaming_etl.streaming_transformations(kafka_stream)

# Step 3: Enrich with static data
dimension_table = spark.read.table("customer_dimensions")
enriched_stream, _ = streaming_etl.streaming_join_operations(windowed_metrics, dimension_table)

# Step 4: Setup multiple sinks
sink_queries = streaming_etl.streaming_sinks_optimization(
    enriched_stream, 
    "/data/analytics", 
    "/checkpoints/analytics"
)

# Step 5: Monitor performance
for query in sink_queries.values():
    streaming_etl.monitoring_streaming_jobs(query)
```

### Example 2: Fraud Detection System
```python
# Real-time fraud detection pipeline
transaction_stream = streaming_etl.kafka_streaming_source(
    kafka_servers="kafka-cluster:9092",
    topics="credit_card_transactions",
    checkpoint_location="/fraud/checkpoints"
)

# Complex fraud detection with multiple patterns
_, high_value_alerts, behavioral_fraud = streaming_etl.streaming_transformations(transaction_stream)

# Combine multiple fraud signals
fraud_alerts = high_value_alerts.union(behavioral_fraud) \
    .withColumn("alert_severity", 
               when(col("amount") > 50000, "CRITICAL")
               .when(col("fraud_score") > 50, "HIGH")
               .otherwise("MEDIUM"))

# Real-time alerting to Kafka
fraud_alert_query = fraud_alerts \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-cluster:9092") \
    .option("topic", "fraud_alerts") \
    .trigger(processingTime="5 seconds") \
    .start()
```

## Streaming Performance Optimization Checklist

| Optimization Area | Action | Expected Benefit |
|------------------|--------|------------------|
| **Source Optimization** | Configure Kafka batch sizes | 2-5x throughput improvement |
| **Watermarking** | Set appropriate late data tolerance | Bounded state size |
| **Triggers** | Choose optimal trigger intervals | Balance latency vs throughput |
| **Checkpointing** | Optimize checkpoint frequency | Faster recovery |
| **Sinks** | Use appropriate output formats | Improved downstream performance |

This comprehensive documentation covers all streaming ETL patterns with practical examples for building robust real-time data pipelines.
