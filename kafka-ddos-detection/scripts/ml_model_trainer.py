#!/usr/bin/env python3
"""
ML Model Trainer using Spark MLlib
Trains anomaly detection models on historical data stored in HDFS
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import ClusteringEvaluator
import argparse
import os

class MLModelTrainer:
    def __init__(self, hdfs_url='hdfs://localhost:9000', 
                 data_path='/ddos/logs/raw',
                 model_path='/ddos/models'):
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("DDoS-ML-Training") \
            .config("spark.hadoop.fs.defaultFS", hdfs_url) \
            .config("spark.sql.shuffle.partitions", 4) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.hdfs_url = hdfs_url
        self.data_path = data_path
        self.model_path = model_path
        
        print("✅ Spark MLlib Session initialized")

    def load_training_data(self, limit_records=None):
        """Load historical network logs from HDFS"""
        print(f"📂 Loading training data from: {self.data_path}")
        
        # Define schema
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
        
        # Read JSON files from HDFS
        df = self.spark.read \
            .schema(schema) \
            .json(f"{self.hdfs_url}{self.data_path}/*/*/*/*/*.json")
        
        if limit_records:
            df = df.limit(limit_records)
        
        count = df.count()
        print(f"✅ Loaded {count} records from HDFS")
        
        return df

    def engineer_features(self, df):
        """Engineer features for ML model"""
        print("🔧 Engineering features...")
        
        # Convert timestamp to datetime
        df = df.withColumn("event_time", to_timestamp(col("timestamp")))
        
        # Time-based features
        df = df.withColumn("hour_of_day", hour(col("event_time"))) \
               .withColumn("day_of_week", dayofweek(col("event_time")))
        
        # Request features
        df = df.withColumn("is_error", when(col("status_code") >= 400, 1).otherwise(0)) \
               .withColumn("is_post", when(col("method") == "POST", 1).otherwise(0)) \
               .withColumn("is_get", when(col("method") == "GET", 1).otherwise(0))
        
        # Aggregate features per IP per time window
        window_spec = window(col("event_time"), "5 minutes")
        
        aggregated = df.groupBy(
            col("source_ip"),
            window_spec.alias("time_window")
        ).agg(
            count("*").alias("request_count"),
            avg("response_time").alias("avg_response_time"),
            sum("is_error").alias("error_count"),
            avg("bytes_sent").alias("avg_bytes_sent"),
            countDistinct("url").alias("url_diversity"),
            max("response_time").alias("max_response_time"),
            stddev("response_time").alias("stddev_response_time")
        )
        
        # Calculate derived metrics
        enriched = aggregated \
            .withColumn("error_rate", col("error_count") / col("request_count")) \
            .withColumn("requests_per_minute", col("request_count") / 5.0) \
            .fillna(0)
        
        print(f"✅ Feature engineering complete")
        enriched.printSchema()
        
        return enriched

    def prepare_features(self, df):
        """Prepare feature vectors for ML"""
        print("📊 Preparing feature vectors...")
        
        # Select features for model
        feature_cols = [
            "request_count",
            "requests_per_minute",
            "avg_response_time",
            "max_response_time",
            "stddev_response_time",
            "error_count",
            "error_rate",
            "avg_bytes_sent",
            "url_diversity"
        ]
        
        # Assemble features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw",
            handleInvalid="skip"
        )
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler])
        
        # Fit and transform
        model = pipeline.fit(df)
        prepared_df = model.transform(df)
        
        print("✅ Features prepared and scaled")
        
        return prepared_df, model, feature_cols

    def train_kmeans_clustering(self, df, k=3):
        """Train KMeans clustering model for anomaly detection"""
        print(f"\n🤖 Training KMeans clustering model (k={k})...")
        
        # Train KMeans
        kmeans = KMeans(k=k, seed=42, maxIter=20)
        model = kmeans.fit(df.select("features"))
        
        # Make predictions
        predictions = model.transform(df)
        
        # Evaluate
        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(predictions)
        
        print(f"✅ KMeans model trained")
        print(f"   Silhouette score: {silhouette:.4f}")
        print(f"   Cluster centers: {k}")
        
        # Show cluster distribution
        cluster_dist = predictions.groupBy("prediction").count().orderBy("prediction")
        print(f"\n📊 Cluster distribution:")
        cluster_dist.show()
        
        # Analyze clusters
        self._analyze_clusters(predictions)
        
        return model, predictions

    def _analyze_clusters(self, predictions):
        """Analyze characteristics of each cluster"""
        print("\n📈 Cluster characteristics:")
        
        cluster_stats = predictions.groupBy("prediction").agg(
            count("*").alias("count"),
            avg("request_count").alias("avg_requests"),
            avg("error_rate").alias("avg_error_rate"),
            avg("avg_response_time").alias("avg_resp_time"),
            avg("url_diversity").alias("avg_url_div")
        ).orderBy("prediction")
        
        cluster_stats.show()
        
        # Identify anomaly cluster (high requests, high errors, low diversity)
        print("\n🔍 Potential anomaly clusters identified:")
        anomaly_clusters = predictions.groupBy("prediction").agg(
            avg("requests_per_minute").alias("rpm"),
            avg("error_rate").alias("err_rate")
        ).filter(
            (col("rpm") > 30) | (col("err_rate") > 0.5)
        )
        
        anomaly_clusters.show()

    def save_model(self, model, model_name="kmeans_anomaly_detector"):
        """Save trained model to HDFS"""
        model_save_path = f"{self.hdfs_url}{self.model_path}/{model_name}"
        
        try:
            print(f"\n💾 Saving model to: {model_save_path}")
            model.write().overwrite().save(model_save_path)
            print(f"✅ Model saved successfully")
            return model_save_path
        except Exception as e:
            print(f"❌ Error saving model: {e}")
            return None

    def load_model(self, model_name="kmeans_anomaly_detector"):
        """Load trained model from HDFS"""
        model_load_path = f"{self.hdfs_url}{self.model_path}/{model_name}"
        
        try:
            print(f"📂 Loading model from: {model_load_path}")
            model = KMeansModel.load(model_load_path)
            print(f"✅ Model loaded successfully")
            return model
        except Exception as e:
            print(f"❌ Error loading model: {e}")
            return None

    def predict_anomalies(self, model, new_data):
        """Use trained model to predict anomalies on new data"""
        print("\n🔮 Making predictions on new data...")
        
        predictions = model.transform(new_data)
        
        # Show sample predictions
        predictions.select(
            "source_ip",
            "request_count",
            "error_rate",
            "avg_response_time",
            "prediction"
        ).show(20)
        
        return predictions

    def train_and_save_model(self, k=3, limit_records=None):
        """Complete training pipeline"""
        print("="*60)
        print("🚀 Starting ML Model Training Pipeline")
        print("="*60)
        
        # Load data
        df = self.load_training_data(limit_records)
        
        # Engineer features
        df_features = self.engineer_features(df)
        
        # Prepare features
        df_prepared, feature_pipeline, feature_cols = self.prepare_features(df_features)
        
        # Train model
        kmeans_model, predictions = self.train_kmeans_clustering(df_prepared, k=k)
        
        # Save models
        self.save_model(feature_pipeline, "feature_pipeline")
        self.save_model(kmeans_model, "kmeans_anomaly_detector")
        
        print("\n" + "="*60)
        print("✅ Training pipeline completed successfully")
        print("="*60)
        
        return kmeans_model, feature_pipeline

    def batch_predict_on_hdfs(self, model_name="kmeans_anomaly_detector"):
        """Run batch predictions on all HDFS data"""
        print("="*60)
        print("🔮 Running Batch Predictions")
        print("="*60)
        
        # Load models
        feature_pipeline = PipelineModel.load(
            f"{self.hdfs_url}{self.model_path}/feature_pipeline"
        )
        kmeans_model = self.load_model(model_name)
        
        if not kmeans_model:
            print("❌ Could not load model")
            return
        
        # Load data
        df = self.load_training_data()
        
        # Engineer features
        df_features = self.engineer_features(df)
        
        # Transform features
        df_prepared = feature_pipeline.transform(df_features)
        
        # Predict
        predictions = self.predict_anomalies(kmeans_model, df_prepared)
        
        # Save predictions
        output_path = f"{self.hdfs_url}{self.data_path}/../predictions"
        print(f"\n💾 Saving predictions to: {output_path}")
        
        predictions.select(
            "source_ip",
            "time_window",
            "request_count",
            "error_rate",
            "avg_response_time",
            "prediction"
        ).write.mode("overwrite").parquet(output_path)
        
        print("✅ Batch predictions completed")

    def cleanup(self):
        """Clean up Spark session"""
        self.spark.stop()
        print("✅ Spark session stopped")

def main():
    parser = argparse.ArgumentParser(description='ML Model Trainer for DDoS Detection')
    parser.add_argument('--hdfs-url', default='hdfs://localhost:9000',
                       help='HDFS URL')
    parser.add_argument('--data-path', default='/ddos/logs/raw',
                       help='Path to training data in HDFS')
    parser.add_argument('--model-path', default='/ddos/models',
                       help='Path to save models in HDFS')
    parser.add_argument('--k-clusters', type=int, default=3,
                       help='Number of clusters for KMeans')
    parser.add_argument('--limit-records', type=int,
                       help='Limit number of records for training')
    parser.add_argument('--mode', choices=['train', 'predict'], default='train',
                       help='Mode: train new model or run predictions')
    
    args = parser.parse_args()
    
    trainer = MLModelTrainer(
        hdfs_url=args.hdfs_url,
        data_path=args.data_path,
        model_path=args.model_path
    )
    
    try:
        if args.mode == 'train':
            trainer.train_and_save_model(
                k=args.k_clusters,
                limit_records=args.limit_records
            )
        else:
            trainer.batch_predict_on_hdfs()
    finally:
        trainer.cleanup()

if __name__ == "__main__":
    main()
