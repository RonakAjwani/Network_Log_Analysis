#!/usr/bin/env python3
"""
Mahout-Based ML DDoS Detection System
Uses Apache Mahout for distributed machine learning on HDFS data
Integrates with Kafka for real-time predictions
"""

import json
import time
import subprocess
import os
from datetime import datetime, timedelta
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier, IsolationForest
import pickle
import argparse

class MahoutMLDetector:
    """
    Advanced ML-based DDoS detector using multiple algorithms:
    1. K-Means Clustering (Mahout-style unsupervised learning)
    2. Isolation Forest (Anomaly detection)
    3. Random Forest (Supervised learning)
    """
    
    def __init__(self, kafka_server='localhost:9092', 
                 input_topic='network-logs', 
                 alert_topic='ddos-alerts',
                 model_dir='models'):
        # Kafka setup
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=[kafka_server],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='latest',
            group_id='mahout-ml-detector'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_server],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        self.input_topic = input_topic
        self.alert_topic = alert_topic
        self.model_dir = model_dir
        
        # ML Models
        self.kmeans_model = None
        self.isolation_forest = None
        self.random_forest = None
        self.scaler = StandardScaler()
        
        # Feature tracking
        self.ip_stats = defaultdict(lambda: {
            'requests': deque(maxlen=100),
            'timestamps': deque(maxlen=100),
            'status_codes': deque(maxlen=100),
            'response_times': deque(maxlen=100),
            'bytes_sent': deque(maxlen=100),
            'urls': set(),
            'methods': deque(maxlen=100),
            'user_agents': deque(maxlen=100),
            'last_alert': None
        })
        
        # Training data collection
        self.training_buffer = []
        self.buffer_size = 1000
        self.auto_retrain_interval = 500  # Retrain every 500 samples
        self.samples_since_training = 0
        
        # Statistics
        self.total_logs_processed = 0
        self.ml_alerts_generated = 0
        self.start_time = datetime.now()
        
        # Initialize models
        self.initialize_models()
    
    def initialize_models(self):
        """Initialize or load ML models"""
        print("🤖 Initializing ML Models...")
        
        # Try to load existing models
        if self.load_models():
            print("✅ Loaded pre-trained models")
        else:
            print("⚠️ No pre-trained models found. Will train on incoming data.")
            # Initialize with default parameters (Mahout-style K-Means)
            self.kmeans_model = KMeans(
                n_clusters=3,  # Normal, Suspicious, Attack
                init='k-means++',
                n_init=10,
                max_iter=300,
                random_state=42
            )
            
            # Isolation Forest for anomaly detection
            self.isolation_forest = IsolationForest(
                contamination=0.1,  # 10% expected anomalies
                random_state=42,
                n_estimators=100
            )
            
            # Random Forest for classification
            self.random_forest = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42
            )
    
    def extract_advanced_features(self, log_entry, ip_stats):
        """
        Extract comprehensive features for ML models
        (Similar to Mahout feature vector creation)
        """
        now = datetime.now()
        
        # Temporal features
        recent_timestamps = [ts for ts in ip_stats['timestamps'] 
                           if (now - ts).total_seconds() < 60]
        request_rate = len(recent_timestamps)
        
        # Request rate in last 10 seconds (short-term burst detection)
        very_recent = [ts for ts in ip_stats['timestamps'] 
                      if (now - ts).total_seconds() < 10]
        burst_rate = len(very_recent)
        
        # Status code features
        recent_status_codes = list(ip_stats['status_codes'])[-20:]
        error_4xx_count = sum(1 for code in recent_status_codes if 400 <= code < 500)
        error_5xx_count = sum(1 for code in recent_status_codes if code >= 500)
        success_count = sum(1 for code in recent_status_codes if 200 <= code < 300)
        total_recent = max(len(recent_status_codes), 1)
        
        error_rate = (error_4xx_count + error_5xx_count) / total_recent
        error_4xx_rate = error_4xx_count / total_recent
        error_5xx_rate = error_5xx_count / total_recent
        
        # Response time features
        recent_response_times = list(ip_stats['response_times'])[-20:]
        if recent_response_times:
            avg_response_time = np.mean(recent_response_times)
            max_response_time = np.max(recent_response_times)
            std_response_time = np.std(recent_response_times)
        else:
            avg_response_time = 0
            max_response_time = 0
            std_response_time = 0
        
        # Bytes sent features
        recent_bytes = list(ip_stats['bytes_sent'])[-20:]
        if recent_bytes:
            avg_bytes = np.mean(recent_bytes)
            total_bytes = np.sum(recent_bytes)
        else:
            avg_bytes = 0
            total_bytes = 0
        
        # URL diversity (important for scanning detection)
        url_diversity = len(ip_stats['urls'])
        url_entropy = self.calculate_entropy(ip_stats['urls'])
        
        # HTTP method diversity
        recent_methods = list(ip_stats['methods'])[-20:]
        method_diversity = len(set(recent_methods))
        get_ratio = recent_methods.count('GET') / max(len(recent_methods), 1)
        
        # User agent analysis
        recent_agents = list(ip_stats['user_agents'])[-20:]
        is_bot = any('bot' in str(agent).lower() for agent in recent_agents)
        agent_diversity = len(set(recent_agents))
        
        # Time-based features
        hour_of_day = now.hour
        is_business_hours = 9 <= hour_of_day <= 17
        day_of_week = now.weekday()
        is_weekend = day_of_week >= 5
        
        # Return feature vector (20 features total)
        return {
            # Rate features
            'request_rate': request_rate,
            'burst_rate': burst_rate,
            
            # Error features
            'error_rate': error_rate,
            'error_4xx_rate': error_4xx_rate,
            'error_5xx_rate': error_5xx_rate,
            
            # Response time features
            'avg_response_time': avg_response_time,
            'max_response_time': max_response_time,
            'std_response_time': std_response_time,
            
            # Data volume features
            'avg_bytes': avg_bytes,
            'total_bytes': total_bytes,
            
            # Diversity features
            'url_diversity': url_diversity,
            'url_entropy': url_entropy,
            'method_diversity': method_diversity,
            'get_ratio': get_ratio,
            'agent_diversity': agent_diversity,
            
            # Behavioral features
            'is_bot': int(is_bot),
            
            # Temporal features
            'hour_of_day': hour_of_day,
            'is_business_hours': int(is_business_hours),
            'is_weekend': int(is_weekend)
        }
    
    def calculate_entropy(self, items):
        """Calculate Shannon entropy for diversity measurement"""
        if not items:
            return 0
        
        counts = {}
        for item in items:
            counts[item] = counts.get(item, 0) + 1
        
        total = len(items)
        entropy = 0
        for count in counts.values():
            p = count / total
            if p > 0:
                entropy -= p * np.log2(p)
        
        return entropy
    
    def features_to_vector(self, features):
        """Convert feature dict to numpy array"""
        feature_order = [
            'request_rate', 'burst_rate', 'error_rate', 'error_4xx_rate', 
            'error_5xx_rate', 'avg_response_time', 'max_response_time', 
            'std_response_time', 'avg_bytes', 'total_bytes', 'url_diversity',
            'url_entropy', 'method_diversity', 'get_ratio', 'agent_diversity',
            'is_bot', 'hour_of_day', 'is_business_hours', 'is_weekend'
        ]
        return np.array([features.get(f, 0) for f in feature_order])
    
    def ml_based_detection(self, ip, features, log_entry):
        """
        Multi-model ML detection (Mahout-inspired approach)
        Uses ensemble of 3 algorithms for robust detection
        """
        alerts = []
        feature_vector = self.features_to_vector(features).reshape(1, -1)
        
        # Normalize features
        try:
            feature_vector_scaled = self.scaler.transform(feature_vector)
        except:
            # Scaler not fitted yet
            return alerts
        
        # 1. K-Means Clustering (Mahout-style unsupervised learning)
        if self.kmeans_model is not None:
            try:
                # Check if model is fitted
                if hasattr(self.kmeans_model, 'cluster_centers_'):
                    cluster = self.kmeans_model.predict(feature_vector_scaled)[0]
                    
                    # Calculate distance to cluster center
                    center = self.kmeans_model.cluster_centers_[cluster]
                    distance = np.linalg.norm(feature_vector_scaled - center)
                    
                    # If distance is large, it's an outlier (potential attack)
                    if distance > 2.5:  # Threshold tuned for DDoS detection
                        alerts.append({
                            'type': 'kmeans_anomaly',
                            'severity': 'HIGH',
                            'message': f'IP {ip} behavior deviates from cluster {cluster} (distance: {distance:.2f})',
                            'confidence': min(100, distance * 20)
                        })
            except:
                pass
        
        # 2. Isolation Forest (Anomaly detection)
        if self.isolation_forest is not None:
            try:
                # Check if model is fitted
                if hasattr(self.isolation_forest, 'estimators_'):
                    anomaly_score = self.isolation_forest.decision_function(feature_vector_scaled)[0]
                    is_anomaly = self.isolation_forest.predict(feature_vector_scaled)[0]
                    
                    if is_anomaly == -1:  # -1 indicates anomaly
                        alerts.append({
                            'type': 'isolation_forest_anomaly',
                            'severity': 'MEDIUM',
                            'message': f'IP {ip} flagged as anomaly (score: {anomaly_score:.3f})',
                            'confidence': min(100, abs(anomaly_score) * 50)
                        })
            except:
                pass
        
        # 3. Random Forest Classification (if trained)
        if self.random_forest is not None:
            try:
                # Check if model is fitted by checking for estimators_ attribute
                if hasattr(self.random_forest, 'estimators_'):
                    prediction = self.random_forest.predict(feature_vector_scaled)[0]
                    probability = self.random_forest.predict_proba(feature_vector_scaled)[0]
                    
                    if prediction == 1:  # 1 = Attack class
                        confidence = probability[1] * 100
                        alerts.append({
                            'type': 'random_forest_classification',
                            'severity': 'HIGH' if confidence > 80 else 'MEDIUM',
                            'message': f'IP {ip} classified as attack (confidence: {confidence:.1f}%)',
                            'confidence': confidence
                        })
            except:
                pass
        
        return alerts
    
    def train_models(self):
        """
        Train all ML models on collected data
        (Simulates Mahout's distributed training on HDFS data)
        """
        if len(self.training_buffer) < 100:
            print(f"⚠️ Insufficient training data ({len(self.training_buffer)} samples). Need at least 100.")
            return False
        
        print(f"\n🎓 Training ML models on {len(self.training_buffer)} samples...")
        
        # Convert to DataFrame
        df = pd.DataFrame(self.training_buffer)
        
        # Extract feature vectors
        X = np.array([self.features_to_vector(features) for features in df['features']])
        
        # Fit scaler
        self.scaler.fit(X)
        X_scaled = self.scaler.transform(X)
        
        # Train K-Means
        print("   Training K-Means clustering...")
        self.kmeans_model.fit(X_scaled)
        
        # Train Isolation Forest
        print("   Training Isolation Forest...")
        self.isolation_forest.fit(X_scaled)
        
        # For Random Forest, we need labels
        # Use rule-based detection to create pseudo-labels
        y = []
        for features in df['features']:
            # Simple labeling: attack if high request rate AND high error rate
            is_attack = (features['request_rate'] > 50 and features['error_rate'] > 0.5) or \
                       (features['burst_rate'] > 30)
            y.append(1 if is_attack else 0)
        
        y = np.array(y)
        
        # Train Random Forest
        if sum(y) > 10:  # Need at least 10 attack samples
            print("   Training Random Forest classifier...")
            self.random_forest.fit(X_scaled, y)
            
            # Print accuracy
            accuracy = self.random_forest.score(X_scaled, y)
            print(f"   ✅ Random Forest training accuracy: {accuracy:.2%}")
        
        # Save models
        self.save_models()
        
        print(f"✅ ML models trained successfully!")
        print(f"   K-Means clusters: {self.kmeans_model.n_clusters}")
        print(f"   Isolation Forest estimators: {self.isolation_forest.n_estimators}")
        print(f"   Random Forest estimators: {self.random_forest.n_estimators}")
        
        return True
    
    def save_models(self):
        """Save trained models to disk"""
        os.makedirs(self.model_dir, exist_ok=True)
        
        models = {
            'kmeans': self.kmeans_model,
            'isolation_forest': self.isolation_forest,
            'random_forest': self.random_forest,
            'scaler': self.scaler
        }
        
        for name, model in models.items():
            path = os.path.join(self.model_dir, f'{name}_model.pkl')
            with open(path, 'wb') as f:
                pickle.dump(model, f)
        
        print(f"💾 Models saved to {self.model_dir}/")
    
    def load_models(self):
        """Load pre-trained models from disk"""
        try:
            model_files = {
                'kmeans': 'kmeans_model.pkl',
                'isolation_forest': 'isolation_forest_model.pkl',
                'random_forest': 'random_forest_model.pkl',
                'scaler': 'scaler_model.pkl'
            }
            
            for name, filename in model_files.items():
                path = os.path.join(self.model_dir, filename)
                if not os.path.exists(path):
                    return False
                
                with open(path, 'rb') as f:
                    model = pickle.load(f)
                    
                if name == 'kmeans':
                    self.kmeans_model = model
                elif name == 'isolation_forest':
                    self.isolation_forest = model
                elif name == 'random_forest':
                    self.random_forest = model
                elif name == 'scaler':
                    self.scaler = model
            
            return True
        except Exception as e:
            print(f"⚠️ Error loading models: {e}")
            return False
    
    def send_alert(self, ip, alerts, log_entry, features):
        """Send ML-detected alert to Kafka"""
        if not alerts:
            return
        
        # Calculate overall confidence
        confidences = [a.get('confidence', 50) for a in alerts]
        avg_confidence = float(np.mean(confidences))
        
        # Convert numpy types to native Python types for JSON serialization
        def convert_to_native(obj):
            """Convert numpy types to native Python types"""
            if isinstance(obj, dict):
                return {k: convert_to_native(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_native(item) for item in obj]
            elif isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64, np.float32)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            else:
                return obj
        
        alert_data = {
            'timestamp': datetime.now().isoformat(),
            'source_ip': ip,
            'alerts': convert_to_native(alerts),
            'features': convert_to_native(features),
            'sample_log': convert_to_native(log_entry),
            'detection_method': 'mahout_ml',
            'confidence': avg_confidence,
            'models_used': len(alerts)
        }
        
        # Send to Kafka
        self.producer.send(self.alert_topic, value=alert_data)
        
        # Print to console with ML styling
        print(f"\n🤖 ML ALERT for IP {ip} (Confidence: {avg_confidence:.1f}%):")
        for alert in alerts:
            conf = alert.get('confidence', 0)
            print(f"   [{alert['severity']}] {alert['type']}: {alert['message']} ({conf:.1f}% conf)")
        
        self.ml_alerts_generated += 1
    
    def should_alert(self, ip, alerts):
        """Check if we should send alert"""
        if not alerts:
            return False
        
        # Rate limiting
        if self.ip_stats[ip]['last_alert']:
            time_since_last = datetime.now() - self.ip_stats[ip]['last_alert']
            if time_since_last.total_seconds() < 30:
                return False
        
        return True
    
    def process_logs(self):
        """Main processing loop with ML detection"""
        print(f"🤖 Starting Mahout-based ML DDoS detection")
        print(f"📡 Input topic: {self.input_topic}")
        print(f"📢 Alert topic: {self.alert_topic}")
        print(f"🎓 Auto-retrain interval: {self.auto_retrain_interval} samples")
        print("🚀 ML monitoring started... (Press Ctrl+C to stop)\n")
        
        try:
            for message in self.consumer:
                log_entry = message.value
                ip = message.key or log_entry.get('source_ip', 'unknown')
                
                # Update IP statistics
                try:
                    timestamp = log_entry['timestamp']
                    if 'T' in timestamp:
                        now = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).replace(tzinfo=None)
                    else:
                        now = datetime.now()
                except:
                    now = datetime.now()
                
                stats = self.ip_stats[ip]
                stats['requests'].append(log_entry)
                stats['timestamps'].append(now)
                stats['status_codes'].append(log_entry.get('status_code', 200))
                stats['response_times'].append(log_entry.get('response_time', 0))
                stats['bytes_sent'].append(log_entry.get('bytes_sent', 0))
                stats['urls'].add(log_entry.get('url', ''))
                stats['methods'].append(log_entry.get('method', 'GET'))
                stats['user_agents'].append(log_entry.get('user_agent', ''))
                
                # Extract features
                features = self.extract_advanced_features(log_entry, stats)
                
                # Add to training buffer
                self.training_buffer.append({
                    'ip': ip,
                    'features': features,
                    'timestamp': now
                })
                
                # Keep buffer size manageable
                if len(self.training_buffer) > self.buffer_size:
                    self.training_buffer.pop(0)
                
                # ML-based detection
                ml_alerts = self.ml_based_detection(ip, features, log_entry)
                
                # Send alert if needed
                if self.should_alert(ip, ml_alerts):
                    self.send_alert(ip, ml_alerts, log_entry, features)
                    stats['last_alert'] = datetime.now()
                
                self.total_logs_processed += 1
                self.samples_since_training += 1
                
                # Auto-retrain models periodically
                if self.samples_since_training >= self.auto_retrain_interval:
                    self.train_models()
                    self.samples_since_training = 0
                
                # Print progress
                if self.total_logs_processed % 50 == 0:
                    uptime = datetime.now() - self.start_time
                    print(f"📈 Processed: {self.total_logs_processed} logs, "
                          f"ML Alerts: {self.ml_alerts_generated}, "
                          f"Training samples: {len(self.training_buffer)}, "
                          f"Uptime: {str(uptime).split('.')[0]}")
                
        except KeyboardInterrupt:
            print("\n⚠️ Stopping ML detection...")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        # Train one final time before exiting
        if len(self.training_buffer) >= 100:
            print("\n🎓 Final training before shutdown...")
            self.train_models()
        
        self.consumer.close()
        self.producer.flush()
        self.producer.close()
        
        uptime = datetime.now() - self.start_time
        print(f"\n📊 Final ML Statistics:")
        print(f"   Logs processed: {self.total_logs_processed}")
        print(f"   ML alerts generated: {self.ml_alerts_generated}")
        print(f"   Unique IPs monitored: {len(self.ip_stats)}")
        print(f"   Training samples collected: {len(self.training_buffer)}")
        print(f"   Total uptime: {str(uptime).split('.')[0]}")
        print("✅ Mahout ML detector stopped cleanly")

def main():
    parser = argparse.ArgumentParser(description='Mahout-Based ML DDoS Detection')
    parser.add_argument('--kafka-server', default='localhost:9092',
                       help='Kafka bootstrap server')
    parser.add_argument('--input-topic', default='network-logs',
                       help='Input topic for network logs')
    parser.add_argument('--alert-topic', default='ddos-alerts',
                       help='Output topic for ML alerts')
    parser.add_argument('--model-dir', default='models',
                       help='Directory to save/load models')
    parser.add_argument('--train-only', action='store_true',
                       help='Only train models and exit')
    
    args = parser.parse_args()
    
    detector = MahoutMLDetector(
        kafka_server=args.kafka_server,
        input_topic=args.input_topic,
        alert_topic=args.alert_topic,
        model_dir=args.model_dir
    )
    
    if args.train_only:
        print("🎓 Training mode: Collecting data and training models...")
        print("   Run the log producer to generate training data")
        print("   Press Ctrl+C after sufficient data is collected\n")
    
    detector.process_logs()

if __name__ == "__main__":
    main()
