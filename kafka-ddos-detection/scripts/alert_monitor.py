#!/usr/bin/env python3
"""
Alert Monitor - Displays DDoS alerts in real-time
"""

import json
from kafka import KafkaConsumer
from datetime import datetime
import argparse

class AlertMonitor:
    def __init__(self, kafka_server='localhost:9092', alert_topic='ddos-alerts'):
        self.consumer = KafkaConsumer(
            alert_topic,
            bootstrap_servers=[kafka_server],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        self.alert_topic = alert_topic
        self.alert_count = 0

    def display_alert(self, alert_data):
        """Display alert in formatted way"""
        self.alert_count += 1
        
        print(f"\n{'='*60}")
        print(f"🚨 ALERT #{self.alert_count}")
        print(f"{'='*60}")
        print(f"⏰ Time: {alert_data['timestamp']}")
        print(f"🌐 Source IP: {alert_data['source_ip']}")
        
        print(f"\n📊 Features:")
        features = alert_data['features']
        print(f"   Request Rate: {features['request_rate']:.1f} req/min")
        print(f"   Error Rate: {features['error_rate']:.2%}")
        print(f"   Avg Response Time: {features['avg_response_time']:.2f}s")
        print(f"   URL Diversity: {features['url_diversity']}")
        
        print(f"\n⚠️ Alerts ({len(alert_data['alerts'])}):")
        for alert in alert_data['alerts']:
            severity_emoji = {'HIGH': '🔴', 'MEDIUM': '🟡', 'LOW': '🟢'}
            emoji = severity_emoji.get(alert['severity'], '⚪')
            print(f"   {emoji} [{alert['severity']}] {alert['type']}")
            print(f"      {alert['message']}")

    def monitor_alerts(self):
        """Main monitoring loop"""
        print(f"🔍 Monitoring alerts from topic: {self.alert_topic}")
        print("📢 Waiting for alerts... (Press Ctrl+C to stop)\n")
        
        try:
            for message in self.consumer:
                alert_data = message.value
                self.display_alert(alert_data)
                
        except KeyboardInterrupt:
            print(f"\n⚠️ Stopping alert monitor...")
        finally:
            self.consumer.close()
            print(f"\n📊 Total alerts monitored: {self.alert_count}")
            print("✅ Alert monitor stopped")

def main():
    parser = argparse.ArgumentParser(description='DDoS Alert Monitor')
    parser.add_argument('--kafka-server', default='localhost:9092',
                       help='Kafka bootstrap server')
    parser.add_argument('--alert-topic', default='ddos-alerts',
                       help='Alert topic to monitor')
    
    args = parser.parse_args()
    
    monitor = AlertMonitor(args.kafka_server, args.alert_topic)
    monitor.monitor_alerts()

if __name__ == "__main__":
    main()
