#!/usr/bin/env python3
"""
HBase Anomaly Storage
Stores detected anomalies and alerts in HBase for fast lookups and historical analysis
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import happybase
import argparse

class HBaseAnomalyStore:
    def __init__(self, kafka_server='localhost:9092', alert_topics=['ddos-alerts', 'spark-alerts'],
                 hbase_host='localhost', hbase_port=9090):
        
        # Kafka consumer setup
        self.consumer = KafkaConsumer(
            *alert_topics,
            bootstrap_servers=[kafka_server],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='hbase-storage'
        )
        
        # HBase connection
        self.hbase_host = hbase_host
        self.hbase_port = hbase_port
        self.connection = None
        self.alerts_table = None
        self.ip_stats_table = None
        
        # Statistics
        self.alerts_stored = 0
        self.start_time = datetime.now()
        
        # Initialize HBase tables
        self._initialize_hbase()

    def _initialize_hbase(self):
        """Initialize HBase connection and create tables"""
        try:
            print(f"🔌 Connecting to HBase at {self.hbase_host}:{self.hbase_port}...")
            self.connection = happybase.Connection(
                host=self.hbase_host,
                port=self.hbase_port,
                timeout=30000
            )
            
            # Create alerts table if it doesn't exist
            if b'ddos_alerts' not in self.connection.tables():
                self.connection.create_table(
                    'ddos_alerts',
                    {
                        'alert': dict(),       # Alert metadata
                        'source': dict(),      # Source IP information
                        'metrics': dict(),     # Detection metrics
                        'features': dict()     # Feature values
                    }
                )
                print("✅ Created HBase table: ddos_alerts")
            else:
                print("✅ HBase table 'ddos_alerts' already exists")
            
            # Create IP statistics table if it doesn't exist
            if b'ip_statistics' not in self.connection.tables():
                self.connection.create_table(
                    'ip_statistics',
                    {
                        'info': dict(),        # IP information
                        'stats': dict(),       # Statistical data
                        'history': dict()      # Historical patterns
                    }
                )
                print("✅ Created HBase table: ip_statistics")
            else:
                print("✅ HBase table 'ip_statistics' already exists")
            
            # Get table references
            self.alerts_table = self.connection.table('ddos_alerts')
            self.ip_stats_table = self.connection.table('ip_statistics')
            
            print("✅ HBase connection established\n")
            
        except Exception as e:
            print(f"❌ Error connecting to HBase: {e}")
            print("⚠️ Make sure HBase Thrift server is running")
            raise

    def generate_alert_rowkey(self, alert_data):
        """Generate unique row key for alert"""
        timestamp = alert_data.get('timestamp', datetime.now().isoformat())
        source_ip = alert_data.get('source_ip', 'unknown')
        
        # Convert timestamp to sortable format
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        ts_key = dt.strftime('%Y%m%d%H%M%S%f')
        
        # Create row key: timestamp_ip (for time-based queries)
        rowkey = f"{ts_key}_{source_ip}".replace('.', '_')
        return rowkey

    def store_alert(self, alert_data):
        """Store alert in HBase"""
        try:
            rowkey = self.generate_alert_rowkey(alert_data)
            
            # Prepare data for HBase
            data = {
                # Alert information
                b'alert:timestamp': str(alert_data.get('timestamp', '')).encode('utf-8'),
                b'alert:source_ip': str(alert_data.get('source_ip', '')).encode('utf-8'),
                b'alert:detection_method': str(alert_data.get('detection_method', 'unknown')).encode('utf-8'),
                b'alert:severity': str(self._get_severity(alert_data)).encode('utf-8'),
                b'alert:alert_types': json.dumps(alert_data.get('alerts', [])).encode('utf-8'),
            }
            
            # Add features if available
            features = alert_data.get('features', {})
            if features:
                for key, value in features.items():
                    column = f'features:{key}'.encode('utf-8')
                    data[column] = str(value).encode('utf-8')
            
            # Add metrics if available (from Spark alerts)
            metrics = alert_data.get('metrics', {})
            if metrics:
                for key, value in metrics.items():
                    column = f'metrics:{key}'.encode('utf-8')
                    data[column] = str(value).encode('utf-8')
            
            # Store in HBase
            self.alerts_table.put(rowkey.encode('utf-8'), data)
            
            # Update IP statistics
            self._update_ip_statistics(alert_data)
            
            self.alerts_stored += 1
            
            return True
            
        except Exception as e:
            print(f"❌ Error storing alert: {e}")
            return False

    def _get_severity(self, alert_data):
        """Extract severity from alert data"""
        # Check direct severity field
        if 'severity' in alert_data:
            return alert_data['severity']
        
        # Check alerts array
        alerts = alert_data.get('alerts', [])
        if alerts:
            # Return highest severity
            severities = [a.get('severity', 'LOW') for a in alerts]
            if 'HIGH' in severities:
                return 'HIGH'
            elif 'MEDIUM' in severities:
                return 'MEDIUM'
            else:
                return 'LOW'
        
        return 'UNKNOWN'

    def _update_ip_statistics(self, alert_data):
        """Update IP statistics table"""
        try:
            source_ip = alert_data.get('source_ip', 'unknown')
            rowkey = source_ip.replace('.', '_').encode('utf-8')
            
            # Get current statistics
            current_data = self.ip_stats_table.row(rowkey)
            
            # Update alert count
            alert_count = int(current_data.get(b'stats:alert_count', b'0'))
            alert_count += 1
            
            # Prepare updated data
            data = {
                b'info:ip_address': source_ip.encode('utf-8'),
                b'info:last_alert_time': str(datetime.now().isoformat()).encode('utf-8'),
                b'stats:alert_count': str(alert_count).encode('utf-8'),
                b'stats:last_severity': str(self._get_severity(alert_data)).encode('utf-8'),
                b'history:latest_alert': json.dumps(alert_data).encode('utf-8')
            }
            
            # Store updated statistics
            self.ip_stats_table.put(rowkey, data)
            
        except Exception as e:
            print(f"⚠️ Error updating IP statistics: {e}")

    def query_alerts_by_ip(self, ip_address, limit=10):
        """Query alerts for a specific IP address"""
        try:
            print(f"\n🔍 Querying alerts for IP: {ip_address}")
            
            results = []
            # Scan table with IP filter
            for key, data in self.alerts_table.scan(limit=limit):
                source_ip = data.get(b'alert:source_ip', b'').decode('utf-8')
                if source_ip == ip_address:
                    alert = {
                        'rowkey': key.decode('utf-8'),
                        'timestamp': data.get(b'alert:timestamp', b'').decode('utf-8'),
                        'source_ip': source_ip,
                        'severity': data.get(b'alert:severity', b'').decode('utf-8'),
                        'detection_method': data.get(b'alert:detection_method', b'').decode('utf-8')
                    }
                    results.append(alert)
            
            print(f"✅ Found {len(results)} alerts")
            for alert in results:
                print(f"   {alert['timestamp']} - [{alert['severity']}] {alert['detection_method']}")
            
            return results
            
        except Exception as e:
            print(f"❌ Error querying alerts: {e}")
            return []

    def get_ip_statistics(self, ip_address):
        """Get statistics for a specific IP"""
        try:
            rowkey = ip_address.replace('.', '_').encode('utf-8')
            data = self.ip_stats_table.row(rowkey)
            
            if not data:
                print(f"ℹ️ No statistics found for IP: {ip_address}")
                return None
            
            stats = {
                'ip_address': data.get(b'info:ip_address', b'').decode('utf-8'),
                'alert_count': int(data.get(b'stats:alert_count', b'0')),
                'last_alert_time': data.get(b'info:last_alert_time', b'').decode('utf-8'),
                'last_severity': data.get(b'stats:last_severity', b'').decode('utf-8')
            }
            
            print(f"\n📊 Statistics for IP: {ip_address}")
            print(f"   Alert count: {stats['alert_count']}")
            print(f"   Last alert: {stats['last_alert_time']}")
            print(f"   Last severity: {stats['last_severity']}")
            
            return stats
            
        except Exception as e:
            print(f"❌ Error getting IP statistics: {e}")
            return None

    def list_top_offenders(self, limit=10):
        """List IPs with most alerts"""
        try:
            print(f"\n🚨 Top {limit} IPs by alert count:")
            
            ip_counts = []
            for key, data in self.ip_stats_table.scan():
                alert_count = int(data.get(b'stats:alert_count', b'0'))
                ip = data.get(b'info:ip_address', b'').decode('utf-8')
                ip_counts.append((ip, alert_count))
            
            # Sort by alert count
            ip_counts.sort(key=lambda x: x[1], reverse=True)
            
            for i, (ip, count) in enumerate(ip_counts[:limit], 1):
                print(f"   {i}. {ip}: {count} alerts")
            
            return ip_counts[:limit]
            
        except Exception as e:
            print(f"❌ Error listing top offenders: {e}")
            return []

    def consume_and_store(self):
        """Main consumption loop"""
        print(f"🔍 Starting HBase storage service...")
        print(f"   Consuming from topics: {self.consumer.subscription()}")
        print("🚀 Monitoring started... (Press Ctrl+C to stop)\n")
        
        try:
            for message in self.consumer:
                alert_data = message.value
                
                # Store alert
                if self.store_alert(alert_data):
                    source_ip = alert_data.get('source_ip', 'unknown')
                    severity = self._get_severity(alert_data)
                    print(f"✅ Stored alert: {source_ip} [{severity}] (Total: {self.alerts_stored})")
                
                # Print statistics every 10 alerts
                if self.alerts_stored % 10 == 0:
                    uptime = datetime.now() - self.start_time
                    print(f"\n📈 Statistics: {self.alerts_stored} alerts stored, "
                          f"Uptime: {str(uptime).split('.')[0]}\n")
                
        except KeyboardInterrupt:
            print("\n⚠️ Stopping HBase storage service...")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        self.consumer.close()
        if self.connection:
            self.connection.close()
        
        uptime = datetime.now() - self.start_time
        print(f"\n📊 Final Statistics:")
        print(f"   Alerts stored: {self.alerts_stored}")
        print(f"   Total uptime: {str(uptime).split('.')[0]}")
        print("✅ HBase storage service stopped cleanly")

def main():
    parser = argparse.ArgumentParser(description='HBase Anomaly Storage Service')
    parser.add_argument('--kafka-server', default='localhost:9092',
                       help='Kafka bootstrap server')
    parser.add_argument('--alert-topics', nargs='+', 
                       default=['ddos-alerts', 'spark-alerts'],
                       help='Kafka topics to consume alerts from')
    parser.add_argument('--hbase-host', default='localhost',
                       help='HBase Thrift server host')
    parser.add_argument('--hbase-port', type=int, default=9090,
                       help='HBase Thrift server port')
    parser.add_argument('--query-ip', type=str,
                       help='Query alerts for specific IP and exit')
    parser.add_argument('--ip-stats', type=str,
                       help='Get statistics for specific IP and exit')
    parser.add_argument('--top-offenders', type=int,
                       help='List top N offending IPs and exit')
    
    args = parser.parse_args()
    
    storage = HBaseAnomalyStore(
        kafka_server=args.kafka_server,
        alert_topics=args.alert_topics,
        hbase_host=args.hbase_host,
        hbase_port=args.hbase_port
    )
    
    # Query mode
    if args.query_ip:
        storage.query_alerts_by_ip(args.query_ip)
    elif args.ip_stats:
        storage.get_ip_statistics(args.ip_stats)
    elif args.top_offenders:
        storage.list_top_offenders(args.top_offenders)
    else:
        # Storage mode
        storage.consume_and_store()

if __name__ == "__main__":
    main()
