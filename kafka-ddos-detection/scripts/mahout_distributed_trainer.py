#!/usr/bin/env python3
"""
Mahout-Style Distributed Machine Learning Trainer
Performs distributed ML model training using Spark on HDFS-stored data
Emulates Apache Mahout's distributed ML capabilities
"""

import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, BisectingKMeans
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import ClusteringEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline, PipelineModel
from hdfs import InsecureClient
import argparse

class MahoutDistributedTrainer:
    """
    Mahout-inspired distributed ML trainer for DDoS detection
    Performs large-scale ML training on HDFS data using Spark
    """
    
    def __init__(self, spark_master='spark://localhost:7077',
                 hdfs_url='http://localhost:9870',
                 hdfs_data_path='/ddos/logs/raw',
                 hdfs_model_path='/ddos/models',
                 local_model_dir='models'):
        
        # Initialize Spark Session with Mahout-like configuration
        self.spark = SparkSession.builder \
            .appName("Mahout-DDoS-ML-Trainer") \
            .master(spark_master) \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # HDFS client
        self.hdfs_client = InsecureClient(hdfs_url, user='root')
        self.hdfs_data_path = hdfs_data_path
        self.hdfs_model_path = hdfs_model_path
        self.local_model_dir = local_model_dir
        
        # Create local model directory
        os.makedirs(local_model_dir, exist_ok=True)
        
        # ML Models
        self.feature_columns = [
            'request_rate', 'error_rate', 'avg_response_time', 
            'max_response_time', 'bytes_sent_total', 'url_diversity',
            'status_4xx_count', 'status_5xx_count', 'hour_of_day'
        ]
        
        print("✅ Mahout Distributed Trainer initialized")
        print(f"   Spark Master: {spark_master}")
        print(f"   HDFS Data: {hdfs_data_path}")
        print(f"   HDFS Models: {hdfs_model_path}")
    
    def load_data_from_hdfs(self, sample_fraction=1.0):
        """
        Load and prepare data from HDFS (Mahout-style data loading)
        """
        print(f"\n📖 Loading data from HDFS: {self.hdfs_data_path}")
        
        try:
            # Define schema for network logs
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
            
            # Read JSON logs from HDFS recursively
            # In Mahout style, we'd use SequenceFiles, but JSON works well for this
            df = self.spark.read.json(
                f"hdfs://namenode:9000{self.hdfs_data_path}/**/*.json",
                schema=schema
            )
            
            # Sample if needed (for faster training)
            if sample_fraction < 1.0:
                df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
            
            count = df.count()
            print(f"✅ Loaded {count:,} log entries from HDFS")
            
            if count == 0:
                print("⚠️ No data found in HDFS. Please run log producer and HDFS storage first.")
                return None
            
            return df
            
        except Exception as e:
            print(f"❌ Error loading data from HDFS: {e}")
            print("⚠️ Make sure HDFS is running and contains data at the specified path")
            return None
    
    def engineer_features(self, df):
        """
        Feature engineering for ML models (Mahout-style feature extraction)
        """
        print("\n🔧 Engineering features...")
        
        # Add timestamp features
        df = df.withColumn("event_timestamp", to_timestamp(col("timestamp")))
        df = df.withColumn("hour_of_day", hour(col("event_timestamp")))
        df = df.withColumn("day_of_week", dayofweek(col("event_timestamp")))
        
        # Aggregate by IP with 5-minute windows
        window_spec = window(col("event_timestamp"), "5 minutes")
        
        features_df = df.groupBy(
            window_spec,
            col("source_ip")
        ).agg(
            count("*").alias("request_count"),
            (count("*") / 5.0).alias("request_rate"),  # per minute
            avg("response_time").alias("avg_response_time"),
            max("response_time").alias("max_response_time"),
            stddev("response_time").alias("stddev_response_time"),
            sum("bytes_sent").alias("bytes_sent_total"),
            countDistinct("url").alias("url_diversity"),
            sum(when(col("status_code").between(400, 499), 1).otherwise(0)).alias("status_4xx_count"),
            sum(when(col("status_code") >= 500, 1).otherwise(0)).alias("status_5xx_count"),
            avg(hour(col("event_timestamp"))).alias("hour_of_day")
        )
        
        # Calculate error rate
        features_df = features_df.withColumn(
            "error_rate",
            (col("status_4xx_count") + col("status_5xx_count")) / col("request_count")
        )
        
        # Fill nulls
        features_df = features_df.fillna(0.0)
        
        # Create labels for supervised learning (simple heuristic)
        features_df = features_df.withColumn(
            "is_attack",
            when(
                (col("request_rate") > 50) & (col("error_rate") > 0.5),
                1.0
            ).otherwise(0.0)
        )
        
        count = features_df.count()
        attack_count = features_df.filter(col("is_attack") == 1.0).count()
        
        print(f"✅ Feature engineering complete")
        print(f"   Total samples: {count:,}")
        print(f"   Attack samples: {attack_count:,} ({attack_count/count*100:.1f}%)")
        print(f"   Normal samples: {count-attack_count:,} ({(count-attack_count)/count*100:.1f}%)")
        
        return features_df
    
    def train_kmeans_clustering(self, features_df):
        """
        Train K-Means clustering model (Mahout's distributed clustering)
        """
        print("\n🎓 Training K-Means Clustering Model...")
        
        # Prepare features
        feature_cols = [col for col in self.feature_columns if col in features_df.columns]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="raw_features"
        )
        
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withMean=True,
            withStd=True
        )
        
        # K-Means with multiple clusters (Mahout-style)
        kmeans = KMeans(
            featuresCol="features",
            predictionCol="cluster",
            k=4,  # 4 clusters: normal, suspicious, attack, heavy-attack
            maxIter=20,
            seed=42
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # Train model
        print("   Training K-Means...")
        model = pipeline.fit(features_df)
        
        # Evaluate
        predictions = model.transform(features_df)
        evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
        silhouette = evaluator.evaluate(predictions)
        
        print(f"✅ K-Means trained successfully")
        print(f"   Silhouette score: {silhouette:.3f}")
        
        # Show cluster distribution
        print("\n   Cluster distribution:")
        cluster_dist = predictions.groupBy("cluster").count().orderBy("cluster").collect()
        for row in cluster_dist:
            print(f"      Cluster {row['cluster']}: {row['count']:,} samples")
        
        return model, "kmeans"
    
    def train_bisecting_kmeans(self, features_df):
        """
        Train Bisecting K-Means (hierarchical clustering, Mahout algorithm)
        """
        print("\n🎓 Training Bisecting K-Means Model...")
        
        feature_cols = [col for col in self.feature_columns if col in features_df.columns]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="raw_features"
        )
        
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withMean=True,
            withStd=True
        )
        
        # Bisecting K-Means (Mahout's hierarchical clustering)
        bisecting_kmeans = BisectingKMeans(
            featuresCol="features",
            predictionCol="cluster",
            k=3,
            maxIter=20,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, bisecting_kmeans])
        
        print("   Training Bisecting K-Means...")
        model = pipeline.fit(features_df)
        
        predictions = model.transform(features_df)
        evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
        silhouette = evaluator.evaluate(predictions)
        
        print(f"✅ Bisecting K-Means trained successfully")
        print(f"   Silhouette score: {silhouette:.3f}")
        
        return model, "bisecting_kmeans"
    
    def train_random_forest(self, features_df):
        """
        Train Random Forest classifier (Mahout's distributed Random Forest)
        """
        print("\n🎓 Training Random Forest Classifier...")
        
        feature_cols = [col for col in self.feature_columns if col in features_df.columns]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        # Random Forest (Mahout-style distributed decision tree ensemble)
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="is_attack",
            predictionCol="prediction",
            numTrees=100,
            maxDepth=10,
            maxBins=32,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, rf])
        
        # Split data
        train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
        
        print(f"   Training set: {train_df.count():,} samples")
        print(f"   Test set: {test_df.count():,} samples")
        print("   Training Random Forest...")
        
        model = pipeline.fit(train_df)
        
        # Evaluate
        predictions = model.transform(test_df)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="is_attack",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        
        # Calculate precision and recall
        precision_evaluator = MulticlassClassificationEvaluator(
            labelCol="is_attack", predictionCol="prediction", metricName="weightedPrecision"
        )
        recall_evaluator = MulticlassClassificationEvaluator(
            labelCol="is_attack", predictionCol="prediction", metricName="weightedRecall"
        )
        
        precision = precision_evaluator.evaluate(predictions)
        recall = recall_evaluator.evaluate(predictions)
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        
        print(f"✅ Random Forest trained successfully")
        print(f"   Accuracy: {accuracy:.3f}")
        print(f"   Precision: {precision:.3f}")
        print(f"   Recall: {recall:.3f}")
        print(f"   F1-Score: {f1:.3f}")
        
        return model, "random_forest"
    
    def train_gradient_boosting(self, features_df):
        """
        Train Gradient Boosted Trees (Mahout-style ensemble learning)
        """
        print("\n🎓 Training Gradient Boosted Trees...")
        
        feature_cols = [col for col in self.feature_columns if col in features_df.columns]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        gbt = GBTClassifier(
            featuresCol="features",
            labelCol="is_attack",
            predictionCol="prediction",
            maxIter=20,
            maxDepth=5,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, gbt])
        
        train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
        
        print("   Training GBT...")
        model = pipeline.fit(train_df)
        
        predictions = model.transform(test_df)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="is_attack",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        
        print(f"✅ GBT trained successfully")
        print(f"   Accuracy: {accuracy:.3f}")
        
        return model, "gradient_boosting"
    
    def save_model_to_hdfs(self, model, model_name):
        """
        Save trained model to HDFS (Mahout-style model storage)
        """
        print(f"\n💾 Saving {model_name} model to HDFS...")
        
        try:
            hdfs_path = f"hdfs://namenode:9000{self.hdfs_model_path}/{model_name}"
            
            # Save model to HDFS
            model.write().overwrite().save(hdfs_path)
            
            # Also save metadata
            metadata = {
                'model_name': model_name,
                'training_time': datetime.now().isoformat(),
                'feature_columns': self.feature_columns,
                'hdfs_path': hdfs_path
            }
            
            metadata_path = f"{self.hdfs_model_path}/{model_name}_metadata.json"
            self.hdfs_client.write(
                metadata_path,
                json.dumps(metadata, indent=2),
                overwrite=True,
                encoding='utf-8'
            )
            
            print(f"✅ Model saved to HDFS: {hdfs_path}")
            print(f"   Metadata: {metadata_path}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error saving model to HDFS: {e}")
            return False
    
    def train_all_models(self, sample_fraction=1.0):
        """
        Train all ML models (Mahout-style batch training)
        """
        print("\n" + "="*70)
        print("🎓 MAHOUT DISTRIBUTED ML TRAINING")
        print("="*70)
        
        # Load data from HDFS
        df = self.load_data_from_hdfs(sample_fraction)
        if df is None:
            return False
        
        # Engineer features
        features_df = self.engineer_features(df)
        if features_df is None:
            return False
        
        # Cache for performance
        features_df.cache()
        
        # Train all models
        models = []
        
        # 1. K-Means Clustering
        try:
            model, name = self.train_kmeans_clustering(features_df)
            self.save_model_to_hdfs(model, name)
            models.append((model, name))
        except Exception as e:
            print(f"❌ K-Means training failed: {e}")
        
        # 2. Bisecting K-Means
        try:
            model, name = self.train_bisecting_kmeans(features_df)
            self.save_model_to_hdfs(model, name)
            models.append((model, name))
        except Exception as e:
            print(f"❌ Bisecting K-Means training failed: {e}")
        
        # 3. Random Forest
        try:
            model, name = self.train_random_forest(features_df)
            self.save_model_to_hdfs(model, name)
            models.append((model, name))
        except Exception as e:
            print(f"❌ Random Forest training failed: {e}")
        
        # 4. Gradient Boosting
        try:
            model, name = self.train_gradient_boosting(features_df)
            self.save_model_to_hdfs(model, name)
            models.append((model, name))
        except Exception as e:
            print(f"❌ Gradient Boosting training failed: {e}")
        
        # Unpersist cached data
        features_df.unpersist()
        
        print("\n" + "="*70)
        print(f"✅ Training complete! Trained {len(models)} models")
        print("="*70)
        
        return True
    
    def cleanup(self):
        """Stop Spark session"""
        print("\n⏹️ Stopping Spark session...")
        self.spark.stop()
        print("✅ Spark session stopped")

def main():
    parser = argparse.ArgumentParser(description='Mahout-Style Distributed ML Trainer')
    parser.add_argument('--spark-master', default='spark://localhost:7077',
                       help='Spark master URL')
    parser.add_argument('--hdfs-url', default='http://localhost:9870',
                       help='HDFS NameNode URL')
    parser.add_argument('--hdfs-data-path', default='/ddos/logs/raw',
                       help='HDFS path containing training data')
    parser.add_argument('--hdfs-model-path', default='/ddos/models',
                       help='HDFS path to save trained models')
    parser.add_argument('--sample-fraction', type=float, default=1.0,
                       help='Fraction of data to use for training (0.0-1.0)')
    parser.add_argument('--local-model-dir', default='models',
                       help='Local directory for model caching')
    
    args = parser.parse_args()
    
    trainer = MahoutDistributedTrainer(
        spark_master=args.spark_master,
        hdfs_url=args.hdfs_url,
        hdfs_data_path=args.hdfs_data_path,
        hdfs_model_path=args.hdfs_model_path,
        local_model_dir=args.local_model_dir
    )
    
    try:
        trainer.train_all_models(sample_fraction=args.sample_fraction)
    except KeyboardInterrupt:
        print("\n⚠️ Training interrupted by user")
    except Exception as e:
        print(f"\n❌ Training error: {e}")
    finally:
        trainer.cleanup()

if __name__ == "__main__":
    main()
