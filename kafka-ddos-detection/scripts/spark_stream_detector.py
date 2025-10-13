#!/usr/bin/env python3
"""
Spark Streaming DDoS Detector
Real-time DDoS detection using Spark Structured Streaming with advanced analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import PipelineModel
import argparse

class SparkStreamDetector:
    def __init__(self, kafka_server='localhost:9092', input_topic='network-logs', 
                 alert_topic='spark-alerts', checkpoint_dir='./spark-checkpoints'):
        
        # Create checkpoint directory if it doesn't exist
        import os
        os.makedirs(checkpoint_dir, exist_ok=True)
        
        # Initialize Spark Session with Kafka package
        self.spark = SparkSession.builder \
            .appName("DDoS-Detection-Streaming") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
            .config("spark.sql.shuffle.partitions", 4) \
            .config("spark.streaming.kafka.maxRatePerPartition", 1000) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.kafka_server = kafka_server
        self.input_topic = input_topic
        self.alert_topic = alert_topic
        
        # Detection thresholds
        self.REQUEST_RATE_THRESHOLD = 50
        self.ERROR_RATE_THRESHOLD = 0.7
        self.RESPONSE_TIME_THRESHOLD = 5.0
        
        print("✅ Spark Streaming Session initialized")

    def define_schema(self):
        """Define schema for network logs"""
        return StructType([
            StructField("timestamp", StringType(), True),
            StructField("source_ip", StringType(), True),
            StructField("method", StringType(), True),
            StructField("url", StringType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("bytes_sent", IntegerType(), True),
            StructField("response_time", DoubleType(), True),
            StructField("user_agent", StringType(), True)
        ])

    def read_kafka_stream(self):
        """Read streaming data from Kafka"""
        schema = self.define_schema()
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON value
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("key").cast("string").alias("source_ip_key"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "source_ip_key", "kafka_timestamp")
        
        return parsed_df

    def detect_anomalies_windowed(self, df):
        """Detect anomalies using windowed aggregations"""
        
        # Add timestamp column
        df_with_ts = df.withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp"))
        )
        
        # Windowed aggregations (5-minute windows, sliding every 1 minute)
        windowed_stats = df_with_ts \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window(col("event_timestamp"), "5 minutes", "1 minute"),
                col("source_ip")
            ) \
            .agg(
                count("*").alias("request_count"),
                avg("response_time").alias("avg_response_time"),
                sum(when(col("status_code") >= 400, 1).otherwise(0)).alias("error_count"),
                countDistinct("url").alias("url_diversity"),
                max("status_code").alias("max_status_code"),
                min("status_code").alias("min_status_code"),
                avg("bytes_sent").alias("avg_bytes_sent")
            )
        
        # Calculate metrics
        enriched_stats = windowed_stats \
            .withColumn("error_rate", col("error_count") / col("request_count")) \
            .withColumn("requests_per_minute", col("request_count") / 5.0)
        
        # Detect anomalies
        alerts = enriched_stats \
            .withColumn("is_high_request_rate", 
                       when(col("requests_per_minute") > self.REQUEST_RATE_THRESHOLD, True).otherwise(False)) \
            .withColumn("is_high_error_rate",
                       when(col("error_rate") > self.ERROR_RATE_THRESHOLD, True).otherwise(False)) \
            .withColumn("is_slow_response",
                       when(col("avg_response_time") > self.RESPONSE_TIME_THRESHOLD, True).otherwise(False)) \
            .withColumn("is_scanning",
                       when((col("requests_per_minute") > 20) & (col("url_diversity") == 1), True).otherwise(False))
        
        # Filter for actual alerts
        detected_alerts = alerts.filter(
            (col("is_high_request_rate") == True) |
            (col("is_high_error_rate") == True) |
            (col("is_slow_response") == True) |
            (col("is_scanning") == True)
        )
        
        # Add severity and alert type
        final_alerts = detected_alerts \
            .withColumn("alert_types",
                       array_remove(array(
                           when(col("is_high_request_rate"), lit("HIGH_REQUEST_RATE")).otherwise(None),
                           when(col("is_high_error_rate"), lit("HIGH_ERROR_RATE")).otherwise(None),
                           when(col("is_slow_response"), lit("SLOW_RESPONSE")).otherwise(None),
                           when(col("is_scanning"), lit("SCANNING_BEHAVIOR")).otherwise(None)
                       ), None)) \
            .withColumn("severity",
                       when(col("is_high_request_rate"), lit("HIGH"))
                       .when(col("is_high_error_rate"), lit("MEDIUM"))
                       .when(col("is_slow_response"), lit("LOW"))
                       .otherwise(lit("MEDIUM"))) \
            .withColumn("detection_method", lit("spark_streaming")) \
            .withColumn("alert_timestamp", current_timestamp())
        
        return final_alerts

    def write_alerts_to_kafka(self, alerts_df):
        """Write alerts to Kafka topic"""
        
        # Create checkpoint directory
        import os
        kafka_checkpoint = "./spark-checkpoints/kafka"
        os.makedirs(kafka_checkpoint, exist_ok=True)
        
        # Prepare alert message
        alert_json = alerts_df.select(
            to_json(struct(
                col("alert_timestamp").alias("timestamp"),
                col("source_ip"),
                col("alert_types"),
                col("severity"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                struct(
                    col("request_count"),
                    col("requests_per_minute"),
                    col("error_rate"),
                    col("avg_response_time"),
                    col("url_diversity"),
                    col("avg_bytes_sent")
                ).alias("metrics"),
                col("detection_method")
            )).alias("value"),
            col("source_ip").alias("key")
        )
        
        # Write to Kafka
        query = alert_json \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server) \
            .option("topic", self.alert_topic) \
            .option("checkpointLocation", kafka_checkpoint) \
            .outputMode("update") \
            .start()
        
        return query

    def write_alerts_to_console(self, alerts_df):
        """Write alerts to console for debugging"""
        
        display_df = alerts_df.select(
            col("alert_timestamp"),
            col("source_ip"),
            col("alert_types"),
            col("severity"),
            col("requests_per_minute"),
            col("error_rate"),
            col("avg_response_time")
        )
        
        query = display_df \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .start()
        
        return query

    def calculate_batch_statistics(self, df):
        """Calculate and display batch statistics"""
        
        stats_query = df \
            .groupBy(window(col("event_timestamp"), "1 minute")) \
            .agg(
                count("*").alias("total_logs"),
                countDistinct("source_ip").alias("unique_ips"),
                avg("response_time").alias("avg_response_time"),
                sum(when(col("status_code") >= 400, 1).otherwise(0)).alias("total_errors")
            ) \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return stats_query

    def run_detection(self, output_mode='kafka'):
        """Run the streaming detection pipeline"""
        print(f"🚀 Starting Spark Streaming DDoS Detection...")
        print(f"   Input topic: {self.input_topic}")
        print(f"   Alert topic: {self.alert_topic}")
        print(f"   Output mode: {output_mode}")
        print(f"   Thresholds: Request Rate={self.REQUEST_RATE_THRESHOLD}, "
              f"Error Rate={self.ERROR_RATE_THRESHOLD}, "
              f"Response Time={self.RESPONSE_TIME_THRESHOLD}")
        
        # Read stream
        stream_df = self.read_kafka_stream()
        
        # Detect anomalies
        alerts_df = self.detect_anomalies_windowed(stream_df)
        
        # Write output
        queries = []
        
        if output_mode == 'kafka':
            kafka_query = self.write_alerts_to_kafka(alerts_df)
            queries.append(kafka_query)
            print("✅ Writing alerts to Kafka")
        elif output_mode == 'console':
            console_query = self.write_alerts_to_console(alerts_df)
            queries.append(console_query)
            print("✅ Writing alerts to console")
        else:  # both
            kafka_query = self.write_alerts_to_kafka(alerts_df)
            console_query = self.write_alerts_to_console(alerts_df)
            queries.append(kafka_query)
            queries.append(console_query)
            print("✅ Writing alerts to Kafka and console")
        
        # Add statistics monitoring
        stats_query = self.calculate_batch_statistics(
            stream_df.withColumn("event_timestamp", to_timestamp(col("timestamp")))
        )
        queries.append(stats_query)
        
        print("🔍 Monitoring started... (Press Ctrl+C to stop)\n")
        
        # Wait for termination
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            print("\n⚠️ Stopping Spark streaming...")
            for query in queries:
                query.stop()
            self.spark.stop()
            print("✅ Spark streaming stopped cleanly")

def main():
    parser = argparse.ArgumentParser(description='Spark Streaming DDoS Detector')
    parser.add_argument('--kafka-server', default='localhost:9092',
                       help='Kafka bootstrap server')
    parser.add_argument('--input-topic', default='network-logs',
                       help='Input Kafka topic for network logs')
    parser.add_argument('--alert-topic', default='spark-alerts',
                       help='Output Kafka topic for alerts')
    parser.add_argument('--output-mode', choices=['kafka', 'console', 'both'], 
                       default='kafka',
                       help='Output mode for alerts')
    parser.add_argument('--checkpoint-dir', default='./spark-checkpoints',
                       help='Checkpoint directory for Spark streaming')
    
    args = parser.parse_args()
    
    detector = SparkStreamDetector(
        kafka_server=args.kafka_server,
        input_topic=args.input_topic,
        alert_topic=args.alert_topic,
        checkpoint_dir=args.checkpoint_dir
    )
    
    detector.run_detection(output_mode=args.output_mode)

if __name__ == "__main__":
    main()
