"""
Spark Streaming ETL and Real-time Data Processing
This module demonstrates streaming ETL patterns and real-time data processing optimization
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *

class SparkStreamingETL:
    def __init__(self, app_name="SparkStreamingETL"):
        """Initialize Spark with streaming optimizations"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.streaming.metricsEnabled", "true") \
            .config("spark.sql.streaming.ui.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation.deleteOnExit", "true") \
            .getOrCreate()

    def kafka_streaming_source(self, kafka_servers, topics, checkpoint_location):
        """Setup Kafka streaming source with optimization"""
        print("\n=== Kafka Streaming Source ===")

        # Read from Kafka with optimized settings
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topics) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 100000) \
            .option("kafka.consumer.session.timeout.ms", 30000) \
            .option("kafka.consumer.heartbeat.interval.ms", 10000) \
            .option("failOnDataLoss", "false") \
            .load()

        # Parse Kafka message
        parsed_df = kafka_df \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                       "topic", "partition", "offset", "timestamp") \
            .withColumn("parsed_value", from_json(col("value"),
                       StructType([
                           StructField("id", IntegerType()),
                           StructField("customer_id", StringType()),
                           StructField("amount", DoubleType()),
                           StructField("transaction_time", StringType()),
                           StructField("category", StringType())
                       ]))) \
            .select("key", "topic", "partition", "offset", "timestamp", "parsed_value.*")

        print("Kafka streaming source configured with parsing")
        return parsed_df

    def streaming_transformations(self, streaming_df):
        """Apply streaming transformations with watermarking"""
        print("\n=== Streaming Transformations ===")

        # Add watermarking for late data handling
        watermarked_df = streaming_df \
            .withWatermark("timestamp", "10 minutes")

        # Real-time aggregations
        windowed_aggregations = watermarked_df \
            .groupBy(
                window(col("timestamp"), "5 minutes", "1 minute"),
                col("category")
            ) \
            .agg(
                sum("amount").alias("total_amount"),
                count("*").alias("transaction_count"),
                avg("amount").alias("avg_amount"),
                countDistinct("customer_id").alias("unique_customers")
            ) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")

        # Real-time filtering and alerting
        high_value_transactions = watermarked_df \
            .filter(col("amount") > 10000) \
            .withColumn("alert_type", lit("HIGH_VALUE")) \
            .withColumn("alert_timestamp", current_timestamp())

        # Fraud detection pattern
        fraud_detection = watermarked_df \
            .groupBy(
                window(col("timestamp"), "1 minute"),
                col("customer_id")
            ) \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount")
            ) \
            .filter((col("transaction_count") > 10) | (col("total_amount") > 50000)) \
            .withColumn("fraud_score",
                       col("transaction_count") * 0.3 + col("total_amount") / 1000 * 0.7) \
            .filter(col("fraud_score") > 20)

        return windowed_aggregations, high_value_transactions, fraud_detection

    def streaming_join_operations(self, streaming_df, static_df):
        """Demonstrate streaming joins with static and streaming data"""
        print("\n=== Streaming Join Operations ===")

        # Stream-static join (dimension lookup)
        enriched_stream = streaming_df \
            .join(broadcast(static_df), "category") \
            .select("id", "customer_id", "amount", "timestamp",
                   "category", "category_description", "category_multiplier")

        # Stream-stream join with watermarking
        # Simulate second stream for join example
        stream2_df = streaming_df \
            .withColumnRenamed("amount", "previous_amount") \
            .withColumn("timestamp", col("timestamp") - expr("INTERVAL 1 MINUTE"))

        stream_stream_join = streaming_df \
            .withWatermark("timestamp", "5 minutes") \
            .join(
                stream2_df.withWatermark("timestamp", "5 minutes"),
                expr("""
                    customer_id = customer_id AND 
                    timestamp >= timestamp - interval 2 minutes AND
                    timestamp <= timestamp + interval 2 minutes
                """)
            )

        return enriched_stream, stream_stream_join

    def streaming_sinks_optimization(self, streaming_df, output_path, checkpoint_path):
        """Optimize different streaming sinks"""
        print("\n=== Streaming Sinks Optimization ===")

        # File sink with partitioning
        file_sink_query = streaming_df \
            .writeStream \
            .format("parquet") \
            .option("path", f"{output_path}/streaming_parquet") \
            .option("checkpointLocation", f"{checkpoint_path}/file_sink") \
            .partitionBy("category") \
            .trigger(processingTime="30 seconds") \
            .outputMode("append") \
            .start()

        # Kafka sink for downstream processing
        kafka_sink_query = streaming_df \
            .select(to_json(struct("*")).alias("value")) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "processed_transactions") \
            .option("checkpointLocation", f"{checkpoint_path}/kafka_sink") \
            .trigger(processingTime="10 seconds") \
            .outputMode("append") \
            .start()

        # Console sink for debugging
        console_query = streaming_df \
            .writeStream \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 20) \
            .trigger(processingTime="10 seconds") \
            .outputMode("append") \
            .start()

        # Memory sink for testing
        memory_query = streaming_df \
            .writeStream \
            .format("memory") \
            .queryName("streaming_memory_table") \
            .outputMode("append") \
            .start()

        return {
            "file_sink": file_sink_query,
            "kafka_sink": kafka_sink_query,
            "console": console_query,
            "memory": memory_query
        }

    def handle_late_data_and_errors(self, streaming_df):
        """Handle late arriving data and streaming errors"""
        print("\n=== Late Data and Error Handling ===")

        # Configure watermarking for late data
        watermarked_df = streaming_df \
            .withWatermark("timestamp", "15 minutes")  # Allow 15 minutes of late data

        # Separate on-time vs late data processing
        current_time_window = window(col("timestamp"), "5 minutes")

        on_time_aggregations = watermarked_df \
            .filter(col("timestamp") >= current_timestamp() - expr("INTERVAL 15 MINUTES")) \
            .groupBy(current_time_window, "category") \
            .agg(sum("amount").alias("on_time_amount"))

        # Error handling with try-catch pattern
        def safe_transformation(df):
            try:
                return df \
                    .withColumn("processed_timestamp", current_timestamp()) \
                    .withColumn("amount_validated",
                               when(col("amount").isNull() | (col("amount") < 0), 0)
                               .otherwise(col("amount")))
            except Exception as e:
                print(f"Transformation error: {e}")
                return df.withColumn("error", lit(str(e)))

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

        return watermarked_df, on_time_aggregations, safe_transformation, create_error_handling_stream

    def streaming_performance_optimization(self, streaming_df):
        """Optimize streaming performance"""
        print("\n=== Streaming Performance Optimization ===")

        # Batch size optimization
        optimized_stream = streaming_df \
            .coalesce(4)  # Reduce partition count for small batches

        # Trigger optimization strategies
        triggers = {
            "processing_time": Trigger.ProcessingTime("30 seconds"),
            "once": Trigger.Once(),  # For testing
            "continuous": Trigger.Continuous("1 second")  # Experimental
        }

        # Checkpoint optimization
        def optimize_checkpoint_settings():
            return {
                "checkpointLocation": "/path/to/checkpoint",
                "cleanSource": "delete",  # Clean up source files after processing
                "maxFilesPerTrigger": 1000,  # Limit files per batch
                "latestFirst": True  # Process newest files first
            }

        # Resource allocation optimization
        def streaming_resource_config():
            return {
                "spark.streaming.backpressure.enabled": "true",
                "spark.streaming.receiver.maxRate": "10000",
                "spark.streaming.kafka.maxRatePerPartition": "1000",
                "spark.streaming.stopGracefullyOnShutdown": "true"
            }

        return optimized_stream, triggers, optimize_checkpoint_settings, streaming_resource_config

    def monitoring_streaming_jobs(self, query):
        """Monitor streaming job performance and health"""
        print("\n=== Streaming Job Monitoring ===")

        # Query progress monitoring
        def monitor_query_progress(streaming_query):
            while streaming_query.isActive:
                progress = streaming_query.lastProgress
                if progress:
                    print(f"Batch ID: {progress.get('batchId', 'N/A')}")
                    print(f"Input Rate: {progress.get('inputRowsPerSecond', 0):.2f} rows/sec")
                    print(f"Processing Rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
                    print(f"Batch Duration: {progress.get('batchDuration', 0)} ms")
                    print(f"Trigger: {progress.get('trigger', 'N/A')}")

                    # Check for performance issues
                    input_rate = progress.get('inputRowsPerSecond', 0)
                    processing_rate = progress.get('processedRowsPerSecond', 0)

                    if input_rate > processing_rate * 1.2:
                        print("WARNING: Input rate exceeding processing rate!")

                    sources = progress.get('sources', [])
                    for source in sources:
                        print(f"Source: {source.get('description', 'Unknown')}")
                        print(f"  Input Rows: {source.get('inputRowsPerSecond', 0)}")
                        print(f"  Start Offset: {source.get('startOffset', 'N/A')}")
                        print(f"  End Offset: {source.get('endOffset', 'N/A')}")

                # Wait before next check
                import time
                time.sleep(10)

        # Exception handling
        def handle_streaming_exceptions(query):
            try:
                query.awaitTermination()
            except Exception as e:
                print(f"Streaming query failed: {e}")
                if query.isActive:
                    query.stop()

        return monitor_query_progress, handle_streaming_exceptions

# Example usage
if __name__ == "__main__":
    streaming_etl = SparkStreamingETL()

    # Create sample streaming DataFrame (simulating Kafka input)
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("category", StringType()),
        StructField("timestamp", TimestampType())
    ])

    # Simulate streaming data
    sample_data = [
        (1, "cust_001", 150.0, "food", "2024-01-01 10:00:00"),
        (2, "cust_002", 75.0, "transport", "2024-01-01 10:01:00"),
        (3, "cust_001", 200.0, "shopping", "2024-01-01 10:02:00")
    ]

    # For demonstration, create a static DataFrame
    static_df = streaming_etl.spark.createDataFrame(sample_data, schema)

    # Create static lookup table
    category_lookup = streaming_etl.spark.createDataFrame([
        ("food", "Food & Dining", 1.0),
        ("transport", "Transportation", 1.2),
        ("shopping", "Shopping", 0.9)
    ], ["category", "category_description", "category_multiplier"])

    # Demonstrate transformations (using static DF for demo)
    windowed_agg, high_value, fraud = streaming_etl.streaming_transformations(static_df)

    # Join operations
    enriched, joined = streaming_etl.streaming_join_operations(static_df, category_lookup)
    enriched.show()

    print("Streaming ETL patterns demonstrated")

    streaming_etl.spark.stop()
