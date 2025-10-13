#!/usr/bin/env python3
"""
Simple DDoS Detector
Consumes network logs from Kafka and detects DDoS attacks using simple rules and ML
"""

import json
import time
from datetime import datetime, timedelta
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer
import argparse

class DDoSDetector:
    def __init__(self, kafka_server='localhost:9092', input_topic='network-logs', alert_topic='ddos-alerts'):
        # Kafka setup
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=[kafka_server],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='latest',
            group_id='ddos-detector'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_server],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        self.input_topic = input_topic
        self.alert_topic = alert_topic
        
        # Detection parameters
        self.request_threshold = 50  # requests per minute per IP
        self.error_rate_threshold = 0.7  # 70% error rate
        self.response_time_threshold = 5.0  # seconds
        
        # Tracking data structures
        self.ip_stats = defaultdict(lambda: {
            'requests': deque(maxlen=100),  # Last 100 requests
            'timestamps': deque(maxlen=100),
            'status_codes': deque(maxlen=100),
            'response_times': deque(maxlen=100),
            'bytes_sent': deque(maxlen=100),
            'urls': set(),
            'last_alert': None
        })
        
        # Statistics
        self.total_logs_processed = 0
        self.alerts_generated = 0
        self.start_time = datetime.now()

    def extract_features(self, log_entry, ip_stats):
        """Extract features for detection"""
        now = datetime.now()
        
        # Calculate request rate (requests per minute)
        recent_timestamps = [ts for ts in ip_stats['timestamps'] 
                           if (now - ts).total_seconds() < 60]
        request_rate = len(recent_timestamps)
        
        # Calculate error rate
        recent_status_codes = list(ip_stats['status_codes'])[-10:]  # Last 10 requests
        error_count = sum(1 for code in recent_status_codes if code >= 400)
        error_rate = error_count / max(1, len(recent_status_codes))
        
        # Calculate average response time
        recent_response_times = list(ip_stats['response_times'])[-10:]
        avg_response_time = sum(recent_response_times) / max(1, len(recent_response_times))
        
        # URL diversity
        url_diversity = len(ip_stats['urls'])
        
        return {
            'request_rate': request_rate,
            'error_rate': error_rate,
            'avg_response_time': avg_response_time,
            'url_diversity': url_diversity,
            'hour_of_day': now.hour,
            'day_of_week': now.weekday()
        }

    def rule_based_detection(self, ip, features, log_entry):
        """Simple rule-based DDoS detection"""
        alerts = []
        
        # High request rate detection
        if features['request_rate'] > self.request_threshold:
            alerts.append({
                'type': 'high_request_rate',
                'severity': 'HIGH',
                'message': f"IP {ip} sending {features['request_rate']} requests/minute (threshold: {self.request_threshold})"
            })
        
        # High error rate detection
        if features['error_rate'] > self.error_rate_threshold:
            alerts.append({
                'type': 'high_error_rate',
                'severity': 'MEDIUM',
                'message': f"IP {ip} has {features['error_rate']:.2%} error rate (threshold: {self.error_rate_threshold:.2%})"
            })
        
        # Slow response time pattern
        if features['avg_response_time'] > self.response_time_threshold:
            alerts.append({
                'type': 'slow_response',
                'severity': 'LOW',
                'message': f"IP {ip} causing {features['avg_response_time']:.2f}s avg response time"
            })
        
        # Low URL diversity (scanning behavior)
        if features['request_rate'] > 20 and features['url_diversity'] == 1:
            alerts.append({
                'type': 'scanning_behavior',
                'severity': 'MEDIUM',
                'message': f"IP {ip} repeatedly accessing single URL with high frequency"
            })
        
        return alerts

    def send_alert(self, ip, alerts, log_entry, features):
        """Send alert to Kafka topic"""
        alert_data = {
            'timestamp': datetime.now().isoformat(),
            'source_ip': ip,
            'alerts': alerts,
            'features': features,
            'sample_log': log_entry,
            'detection_method': 'rule_based'
        }
        
        # Send to Kafka
        self.producer.send(self.alert_topic, value=alert_data)
        
        # Print to console
        print(f"\n🚨 ALERT for IP {ip}:")
        for alert in alerts:
            print(f"   [{alert['severity']}] {alert['type']}: {alert['message']}")
        
        self.alerts_generated += 1

    def should_alert(self, ip, alerts):
        """Check if we should send alert (avoid spam)"""
        if not alerts:
            return False
        
        # Rate limiting: don't alert for same IP within 30 seconds
        if self.ip_stats[ip]['last_alert']:
            time_since_last = datetime.now() - self.ip_stats[ip]['last_alert']
            if time_since_last.total_seconds() < 30:
                return False
        
        return True

    def process_logs(self):
        """Main processing loop"""
        print(f"🔍 Starting DDoS detection on topic: {self.input_topic}")
        print(f"📢 Alerts will be sent to: {self.alert_topic}")
        print("🚀 Monitoring started... (Press Ctrl+C to stop)")
        
        try:
            for message in self.consumer:
                log_entry = message.value
                ip = message.key or log_entry.get('source_ip', 'unknown')
                
                # Update IP statistics
                now = datetime.fromisoformat(log_entry['timestamp'].replace('Z', '+00:00')).replace(tzinfo=None)
                stats = self.ip_stats[ip]
                
                stats['requests'].append(log_entry)
                stats['timestamps'].append(now)
                stats['status_codes'].append(log_entry['status_code'])
                stats['response_times'].append(log_entry['response_time'])
                stats['bytes_sent'].append(log_entry['bytes_sent'])
                stats['urls'].add(log_entry['url'])
                
                # Extract features
                features = self.extract_features(log_entry, stats)
                
                # Rule-based detection
                rule_alerts = self.rule_based_detection(ip, features, log_entry)
                
                # Send alert if needed
                if self.should_alert(ip, rule_alerts):
                    self.send_alert(ip, rule_alerts, log_entry, features)
                    stats['last_alert'] = datetime.now()
                
                self.total_logs_processed += 1
                
                # Print progress every 50 logs
                if self.total_logs_processed % 50 == 0:
                    uptime = datetime.now() - self.start_time
                    print(f"📈 Processed: {self.total_logs_processed} logs, "
                          f"Alerts: {self.alerts_generated}, "
                          f"Uptime: {str(uptime).split('.')[0]}")
                
        except KeyboardInterrupt:
            print("\n⚠️ Stopping detection...")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        self.consumer.close()
        self.producer.flush()
        self.producer.close()
        
        uptime = datetime.now() - self.start_time
        print(f"\n📊 Final Statistics:")
        print(f"   Logs processed: {self.total_logs_processed}")
        print(f"   Alerts generated: {self.alerts_generated}")
        print(f"   Unique IPs monitored: {len(self.ip_stats)}")
        print(f"   Total uptime: {str(uptime).split('.')[0]}")
        print("✅ DDoS detector stopped cleanly")

def main():
    parser = argparse.ArgumentParser(description='DDoS Detection System')
    parser.add_argument('--kafka-server', default='localhost:9092',
                       help='Kafka bootstrap server')
    parser.add_argument('--input-topic', default='network-logs',
                       help='Input topic for network logs')
    parser.add_argument('--alert-topic', default='ddos-alerts',
                       help='Output topic for alerts')
    
    args = parser.parse_args()
    
    detector = DDoSDetector(
        kafka_server=args.kafka_server,
        input_topic=args.input_topic,
        alert_topic=args.alert_topic
    )
    
    detector.process_logs()

if __name__ == "__main__":
    main()
