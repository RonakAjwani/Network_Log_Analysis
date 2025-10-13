#!/usr/bin/env python3
"""
Big Data Analytics with Spark and HDFS
Performs large-scale pattern detection, trend analysis, and batch analytics
on network logs stored in HDFS using Spark SQL and DataFrames
"""

import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from hdfs import InsecureClient
import argparse

class BigDataAnalyzer:
    """
    Large-scale network log analyzer using Spark and HDFS
    Performs comprehensive analytics for DDoS detection and threat intelligence
    """
    
    def __init__(self, spark_master='spark://localhost:7077',
                 hdfs_url='http://localhost:9870',
                 hdfs_data_path='/ddos/logs/raw'):
        
        # Initialize Spark Session with big data configuration
        self.spark = SparkSession.builder \
            .appName("BigData-DDoS-Analytics") \
            .master(spark_master) \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # HDFS client
        self.hdfs_client = InsecureClient(hdfs_url, user='root')
        self.hdfs_data_path = hdfs_data_path
        
        print("✅ Big Data Analyzer initialized")
        print(f"   Spark Master: {spark_master}")
        print(f"   HDFS Data Path: {hdfs_data_path}")
    
    def load_data(self):
        """Load network logs from HDFS"""
        print(f"\n📖 Loading data from HDFS: {self.hdfs_data_path}")
        
        try:
            schema = StructType([
                StructField("timestamp", StringType(), True),
                StructField("source_ip", StringType(), True),
                StructField("method", StringType(), True),
                StructField("url", StringType(), True),
                StructField("status_code", IntegerType(), True),
                StructField("bytes_sent", IntegerType(), True),
                StructField("response_time", DoubleType(), True),
                StructField("user_agent", StringType(), True)
            ])
            
            df = self.spark.read.json(
                f"hdfs://namenode:9000{self.hdfs_data_path}/**/*.json",
                schema=schema
            )
            
            # Add derived columns
            df = df.withColumn("event_timestamp", to_timestamp(col("timestamp")))
            df = df.withColumn("date", to_date(col("event_timestamp")))
            df = df.withColumn("hour", hour(col("event_timestamp")))
            df = df.withColumn("day_of_week", dayofweek(col("event_timestamp")))
            
            count = df.count()
            print(f"✅ Loaded {count:,} log entries")
            
            if count == 0:
                print("⚠️ No data found. Run log producer and HDFS storage first.")
                return None
            
            # Show sample
            print("\n📋 Sample data:")
            df.select("source_ip", "method", "status_code", "response_time", "hour").show(5)
            
            return df
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return None
    
    def analyze_traffic_patterns(self, df):
        """Analyze overall traffic patterns (MapReduce-style aggregation)"""
        print("\n" + "="*70)
        print("📊 TRAFFIC PATTERN ANALYSIS")
        print("="*70)
        
        # Overall statistics
        print("\n1. Overall Statistics:")
        total_requests = df.count()
        unique_ips = df.select("source_ip").distinct().count()
        date_range = df.select(min("date"), max("date")).first()
        
        print(f"   Total Requests: {total_requests:,}")
        print(f"   Unique IPs: {unique_ips:,}")
        print(f"   Date Range: {date_range[0]} to {date_range[1]}")
        print(f"   Avg Requests per IP: {total_requests/unique_ips:.1f}")
        
        # Requests by hour (daily pattern)
        print("\n2. Traffic by Hour of Day:")
        hourly_traffic = df.groupBy("hour") \
            .agg(count("*").alias("request_count")) \
            .orderBy("hour")
        
        hourly_traffic.show()
        
        # Requests by day of week
        print("\n3. Traffic by Day of Week:")
        daily_traffic = df.groupBy("day_of_week") \
            .agg(count("*").alias("request_count")) \
            .orderBy("day_of_week")
        
        daily_traffic.show()
        
        # HTTP method distribution
        print("\n4. HTTP Method Distribution:")
        method_dist = df.groupBy("method") \
            .agg(count("*").alias("count")) \
            .orderBy(desc("count"))
        
        method_dist.show()
        
        # Status code distribution
        print("\n5. Status Code Distribution:")
        status_dist = df.groupBy("status_code") \
            .agg(count("*").alias("count")) \
            .orderBy(desc("count"))
        
        status_dist.show()
        
        return True
    
    def detect_suspicious_ips(self, df, threshold=100):
        """Detect suspicious IPs using aggregation (MapReduce pattern)"""
        print("\n" + "="*70)
        print(f"🔍 SUSPICIOUS IP DETECTION (threshold: {threshold} req/min)")
        print("="*70)
        
        # Aggregate by IP
        ip_stats = df.groupBy("source_ip").agg(
            count("*").alias("total_requests"),
            countDistinct("url").alias("unique_urls"),
            avg("response_time").alias("avg_response_time"),
            max("response_time").alias("max_response_time"),
            sum(when(col("status_code") >= 400, 1).otherwise(0)).alias("error_count"),
            sum(when(col("status_code") >= 500, 1).otherwise(0)).alias("server_error_count"),
            sum("bytes_sent").alias("total_bytes"),
            min("event_timestamp").alias("first_seen"),
            max("event_timestamp").alias("last_seen")
        )
        
        # Calculate derived metrics
        ip_stats = ip_stats.withColumn(
            "error_rate",
            col("error_count") / col("total_requests")
        )
        
        ip_stats = ip_stats.withColumn(
            "duration_seconds",
            unix_timestamp(col("last_seen")) - unix_timestamp(col("first_seen"))
        )
        
        ip_stats = ip_stats.withColumn(
            "requests_per_minute",
            when(col("duration_seconds") > 0,
                 col("total_requests") / (col("duration_seconds") / 60))
            .otherwise(col("total_requests"))
        )
        
        # Detect suspicious patterns
        suspicious_ips = ip_stats.filter(
            (col("requests_per_minute") > threshold) |
            ((col("error_rate") > 0.7) & (col("total_requests") > 50)) |
            ((col("unique_urls") == 1) & (col("total_requests") > 100))
        )
        
        suspicious_count = suspicious_ips.count()
        
        print(f"\n🚨 Found {suspicious_count} suspicious IPs")
        
        if suspicious_count > 0:
            print("\nTop 20 Suspicious IPs:")
            suspicious_ips.select(
                "source_ip",
                "total_requests",
                "requests_per_minute",
                "error_rate",
                "unique_urls"
            ).orderBy(desc("requests_per_minute")).show(20, truncate=False)
        
        return suspicious_ips
    
    def analyze_attack_patterns(self, df):
        """Analyze DDoS attack patterns using windowed aggregations"""
        print("\n" + "="*70)
        print("⚡ ATTACK PATTERN ANALYSIS")
        print("="*70)
        
        # Time-based aggregation (5-minute windows)
        window_stats = df.groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("source_ip")
        ).agg(
            count("*").alias("request_count"),
            avg("response_time").alias("avg_response_time"),
            sum(when(col("status_code") >= 500, 1).otherwise(0)).alias("server_errors")
        )
        
        # Find attack windows
        attacks = window_stats.filter(
            (col("request_count") > 250) |  # > 50 req/min
            ((col("server_errors") > 50) & (col("avg_response_time") > 5.0))
        )
        
        attack_count = attacks.count()
        
        print(f"\n🔥 Detected {attack_count} attack windows")
        
        if attack_count > 0:
            print("\nAttack Windows:")
            attacks.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "source_ip",
                "request_count",
                "avg_response_time",
                "server_errors"
            ).orderBy(desc("request_count")).show(20, truncate=False)
        
        return attacks
    
    def analyze_url_patterns(self, df):
        """Analyze URL access patterns to detect scanning behavior"""
        print("\n" + "="*70)
        print("🌐 URL ACCESS PATTERN ANALYSIS")
        print("="*70)
        
        # Most accessed URLs
        print("\n1. Top 20 Most Accessed URLs:")
        url_stats = df.groupBy("url").agg(
            count("*").alias("access_count"),
            countDistinct("source_ip").alias("unique_ips"),
            avg("response_time").alias("avg_response_time"),
            sum(when(col("status_code") == 404, 1).otherwise(0)).alias("not_found_count")
        ).orderBy(desc("access_count"))
        
        url_stats.show(20, truncate=False)
        
        # URLs with high 404 rates (potential scanning)
        print("\n2. URLs with High 404 Rates (Scanning Detection):")
        scanning_urls = url_stats.filter(
            (col("not_found_count") > 10) &
            (col("not_found_count") / col("access_count") > 0.5)
        ).orderBy(desc("not_found_count"))
        
        scanning_count = scanning_urls.count()
        if scanning_count > 0:
            print(f"   Found {scanning_count} potentially scanned URLs")
            scanning_urls.show(10, truncate=False)
        else:
            print("   No scanning patterns detected")
        
        return url_stats
    
    def analyze_response_times(self, df):
        """Analyze response time patterns for performance degradation"""
        print("\n" + "="*70)
        print("⏱️  RESPONSE TIME ANALYSIS")
        print("="*70)
        
        # Overall response time statistics
        response_stats = df.select(
            avg("response_time").alias("avg_response_time"),
            percentile_approx("response_time", 0.5).alias("median_response_time"),
            percentile_approx("response_time", 0.95).alias("p95_response_time"),
            percentile_approx("response_time", 0.99).alias("p99_response_time"),
            max("response_time").alias("max_response_time")
        ).first()
        
        print("\n1. Response Time Statistics:")
        print(f"   Average: {response_stats['avg_response_time']:.3f}s")
        print(f"   Median (P50): {response_stats['median_response_time']:.3f}s")
        print(f"   P95: {response_stats['p95_response_time']:.3f}s")
        print(f"   P99: {response_stats['p99_response_time']:.3f}s")
        print(f"   Max: {response_stats['max_response_time']:.3f}s")
        
        # Response times by hour
        print("\n2. Average Response Time by Hour:")
        hourly_response = df.groupBy("hour") \
            .agg(
                avg("response_time").alias("avg_response_time"),
                percentile_approx("response_time", 0.95).alias("p95_response_time"),
                count("*").alias("request_count")
            ) \
            .orderBy("hour")
        
        hourly_response.show()
        
        # Slow requests (potential DoS impact)
        slow_threshold = response_stats['p95_response_time']
        slow_requests = df.filter(col("response_time") > slow_threshold)
        slow_count = slow_requests.count()
        
        print(f"\n3. Slow Requests (> {slow_threshold:.2f}s):")
        print(f"   Total slow requests: {slow_count:,}")
        
        if slow_count > 0:
            print("\n   Top IPs causing slow requests:")
            slow_requests.groupBy("source_ip") \
                .agg(count("*").alias("slow_request_count")) \
                .orderBy(desc("slow_request_count")) \
                .show(10)
        
        return response_stats
    
    def generate_threat_intelligence(self, df):
        """Generate threat intelligence report"""
        print("\n" + "="*70)
        print("🛡️  THREAT INTELLIGENCE REPORT")
        print("="*70)
        
        # High-risk IP profile
        print("\n1. High-Risk IP Profiles:")
        
        risk_profiles = df.groupBy("source_ip").agg(
            count("*").alias("total_requests"),
            sum(when(col("status_code").between(400, 499), 1).otherwise(0)).alias("client_errors"),
            sum(when(col("status_code") >= 500, 1).otherwise(0)).alias("server_errors"),
            countDistinct("url").alias("unique_urls"),
            countDistinct("user_agent").alias("unique_agents"),
            avg("response_time").alias("avg_response_time"),
            max("response_time").alias("max_response_time")
        )
        
        # Calculate risk score
        risk_profiles = risk_profiles.withColumn(
            "risk_score",
            (
                (col("total_requests") / 10) +  # Volume factor
                (col("server_errors") * 2) +     # Server impact
                (when(col("unique_urls") == 1, 50).otherwise(0)) +  # Scanning
                (col("max_response_time") * 5)   # Performance impact
            )
        )
        
        high_risk = risk_profiles.filter(col("risk_score") > 100) \
            .orderBy(desc("risk_score"))
        
        high_risk_count = high_risk.count()
        
        print(f"   High-risk IPs: {high_risk_count}")
        
        if high_risk_count > 0:
            high_risk.select(
                "source_ip",
                "total_requests",
                "server_errors",
                "unique_urls",
                "risk_score"
            ).show(20, truncate=False)
        
        # Bot detection
        print("\n2. Potential Bot Traffic:")
        
        bot_ips = df.filter(
            lower(col("user_agent")).contains("bot") |
            lower(col("user_agent")).contains("crawler") |
            lower(col("user_agent")).contains("spider") |
            lower(col("user_agent")).contains("curl") |
            lower(col("user_agent")).contains("wget")
        ).select("source_ip", "user_agent").distinct()
        
        bot_count = bot_ips.count()
        print(f"   Potential bots detected: {bot_count}")
        
        if bot_count > 0:
            bot_ips.show(20, truncate=False)
        
        return risk_profiles
    
    def run_comprehensive_analysis(self):
        """Run all analytics in sequence"""
        print("\n" + "="*80)
        print("🎯 COMPREHENSIVE BIG DATA ANALYSIS")
        print("="*80)
        print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        start_time = datetime.now()
        
        # Load data
        df = self.load_data()
        if df is None:
            return False
        
        # Cache for performance
        df.cache()
        
        # Run all analyses
        try:
            self.analyze_traffic_patterns(df)
            self.detect_suspicious_ips(df, threshold=50)
            self.analyze_attack_patterns(df)
            self.analyze_url_patterns(df)
            self.analyze_response_times(df)
            self.generate_threat_intelligence(df)
            
        except Exception as e:
            print(f"\n❌ Analysis error: {e}")
            return False
        finally:
            df.unpersist()
        
        duration = datetime.now() - start_time
        
        print("\n" + "="*80)
        print(f"✅ ANALYSIS COMPLETE")
        print(f"Duration: {str(duration).split('.')[0]}")
        print("="*80)
        
        return True
    
    def cleanup(self):
        """Stop Spark session"""
        print("\n⏹️ Stopping Spark session...")
        self.spark.stop()
        print("✅ Spark session stopped")

def main():
    parser = argparse.ArgumentParser(description='Big Data Analytics with Spark and HDFS')
    parser.add_argument('--spark-master', default='spark://localhost:7077',
                       help='Spark master URL')
    parser.add_argument('--hdfs-url', default='http://localhost:9870',
                       help='HDFS NameNode URL')
    parser.add_argument('--hdfs-data-path', default='/ddos/logs/raw',
                       help='HDFS path containing log data')
    parser.add_argument('--analysis', choices=[
        'all', 'traffic', 'suspicious', 'attacks', 'urls', 'response', 'threat'
    ], default='all', help='Type of analysis to run')
    
    args = parser.parse_args()
    
    analyzer = BigDataAnalyzer(
        spark_master=args.spark_master,
        hdfs_url=args.hdfs_url,
        hdfs_data_path=args.hdfs_data_path
    )
    
    try:
        if args.analysis == 'all':
            analyzer.run_comprehensive_analysis()
        else:
            df = analyzer.load_data()
            if df:
                df.cache()
                
                if args.analysis == 'traffic':
                    analyzer.analyze_traffic_patterns(df)
                elif args.analysis == 'suspicious':
                    analyzer.detect_suspicious_ips(df)
                elif args.analysis == 'attacks':
                    analyzer.analyze_attack_patterns(df)
                elif args.analysis == 'urls':
                    analyzer.analyze_url_patterns(df)
                elif args.analysis == 'response':
                    analyzer.analyze_response_times(df)
                elif args.analysis == 'threat':
                    analyzer.generate_threat_intelligence(df)
                
                df.unpersist()
                
    except KeyboardInterrupt:
        print("\n⚠️ Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        analyzer.cleanup()

if __name__ == "__main__":
    main()
