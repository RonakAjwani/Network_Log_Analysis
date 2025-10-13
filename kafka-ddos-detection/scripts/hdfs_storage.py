#!/usr/bin/env python3
"""
HDFS Storage Integration
Consumes network logs from Kafka and stores them in HDFS for long-term analysis
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
import argparse
import os

class HDFSStorage:
    def __init__(self, kafka_server='localhost:9092', hdfs_url='http://localhost:9870', 
                 input_topic='network-logs', hdfs_base_path='/ddos/logs'):
        # Kafka consumer setup
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=[kafka_server],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='earliest',
            group_id='hdfs-storage'
        )
        
        # HDFS client setup
        self.hdfs_client = InsecureClient(hdfs_url, user='root')
        self.hdfs_base_path = hdfs_base_path
        self.input_topic = input_topic
        
        # Buffering for batch writes
        self.buffer = []
        self.buffer_size = 100  # Write to HDFS every 100 logs
        
        # Statistics
        self.logs_stored = 0
        self.batches_written = 0
        self.start_time = datetime.now()
        
        # Initialize HDFS directory structure
        self._initialize_hdfs_structure()

    def _initialize_hdfs_structure(self):
        """Create HDFS directory structure if it doesn't exist"""
        try:
            # Create base directory
            self.hdfs_client.makedirs(self.hdfs_base_path)
            print(f"✅ HDFS base directory created/verified: {self.hdfs_base_path}")
            
            # Create subdirectories for organization
            subdirs = ['raw', 'processed', 'alerts', 'models']
            for subdir in subdirs:
                path = f"{self.hdfs_base_path}/{subdir}"
                self.hdfs_client.makedirs(path)
                print(f"   Created subdirectory: {path}")
                
        except Exception as e:
            print(f"⚠️ Error initializing HDFS structure: {e}")

    def get_hdfs_path(self, log_entry):
        """Generate HDFS path with date partitioning"""
        timestamp = datetime.fromisoformat(log_entry['timestamp'].replace('Z', '+00:00'))
        year = timestamp.strftime('%Y')
        month = timestamp.strftime('%m')
        day = timestamp.strftime('%d')
        hour = timestamp.strftime('%H')
        
        # Partition by date and hour for efficient querying
        path = f"{self.hdfs_base_path}/raw/year={year}/month={month}/day={day}/hour={hour}"
        return path

    def write_batch_to_hdfs(self, logs_by_partition):
        """Write buffered logs to HDFS"""
        try:
            for path, logs in logs_by_partition.items():
                # Ensure directory exists
                self.hdfs_client.makedirs(path)
                
                # Create filename with timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{path}/logs_{timestamp}.json"
                
                # Write logs as JSON lines
                with self.hdfs_client.write(filename, encoding='utf-8') as writer:
                    for log in logs:
                        writer.write(json.dumps(log) + '\n')
                
                self.batches_written += 1
                print(f"📦 Batch {self.batches_written} written to: {filename} ({len(logs)} logs)")
                
        except Exception as e:
            print(f"❌ Error writing to HDFS: {e}")

    def flush_buffer(self):
        """Flush buffer to HDFS"""
        if not self.buffer:
            return
        
        # Group logs by partition path
        logs_by_partition = {}
        for log in self.buffer:
            path = self.get_hdfs_path(log)
            if path not in logs_by_partition:
                logs_by_partition[path] = []
            logs_by_partition[path].append(log)
        
        # Write to HDFS
        self.write_batch_to_hdfs(logs_by_partition)
        
        # Update statistics
        self.logs_stored += len(self.buffer)
        
        # Clear buffer
        self.buffer.clear()

    def consume_and_store(self):
        """Main consumption loop"""
        print(f"🔍 Starting HDFS storage service...")
        print(f"   Kafka topic: {self.input_topic}")
        print(f"   HDFS base path: {self.hdfs_base_path}")
        print(f"   Buffer size: {self.buffer_size}")
        print("🚀 Monitoring started... (Press Ctrl+C to stop)\n")
        
        try:
            for message in self.consumer:
                log_entry = message.value
                
                # Add to buffer
                self.buffer.append(log_entry)
                
                # Flush buffer if it reaches threshold
                if len(self.buffer) >= self.buffer_size:
                    self.flush_buffer()
                
                # Print progress every 500 logs
                if self.logs_stored > 0 and self.logs_stored % 500 == 0:
                    uptime = datetime.now() - self.start_time
                    rate = self.logs_stored / uptime.total_seconds()
                    print(f"📈 Stored: {self.logs_stored} logs, "
                          f"Batches: {self.batches_written}, "
                          f"Rate: {rate:.1f} logs/sec")
                
        except KeyboardInterrupt:
            print("\n⚠️ Stopping HDFS storage service...")
        finally:
            # Flush remaining logs
            if self.buffer:
                print("📦 Flushing remaining logs...")
                self.flush_buffer()
            
            self.cleanup()

    def list_stored_data(self, max_files=10):
        """List stored data in HDFS"""
        try:
            print(f"\n📂 Stored data in HDFS ({self.hdfs_base_path}):")
            
            # Walk through HDFS directory
            file_count = 0
            for root, dirs, files in self.hdfs_client.walk(self.hdfs_base_path):
                for file in files:
                    if file_count >= max_files:
                        print(f"   ... and more files")
                        return
                    
                    path = f"{root}/{file}"
                    status = self.hdfs_client.status(path)
                    size_mb = status['length'] / (1024 * 1024)
                    
                    print(f"   📄 {path}")
                    print(f"      Size: {size_mb:.2f} MB, Modified: {status['modificationTime']}")
                    
                    file_count += 1
                    
        except Exception as e:
            print(f"❌ Error listing HDFS data: {e}")

    def cleanup(self):
        """Clean up resources"""
        self.consumer.close()
        
        uptime = datetime.now() - self.start_time
        print(f"\n📊 Final Statistics:")
        print(f"   Logs stored: {self.logs_stored}")
        print(f"   Batches written: {self.batches_written}")
        print(f"   Total uptime: {str(uptime).split('.')[0]}")
        print(f"   Average rate: {self.logs_stored / max(1, uptime.total_seconds()):.1f} logs/sec")
        print("✅ HDFS storage service stopped cleanly")

def main():
    parser = argparse.ArgumentParser(description='HDFS Storage Service for Network Logs')
    parser.add_argument('--kafka-server', default='localhost:9092',
                       help='Kafka bootstrap server')
    parser.add_argument('--hdfs-url', default='http://localhost:9870',
                       help='HDFS NameNode URL')
    parser.add_argument('--input-topic', default='network-logs',
                       help='Kafka topic to consume from')
    parser.add_argument('--hdfs-path', default='/ddos/logs',
                       help='Base path in HDFS for storing logs')
    parser.add_argument('--buffer-size', type=int, default=100,
                       help='Number of logs to buffer before writing')
    parser.add_argument('--list', action='store_true',
                       help='List stored data and exit')
    
    args = parser.parse_args()
    
    storage = HDFSStorage(
        kafka_server=args.kafka_server,
        hdfs_url=args.hdfs_url,
        input_topic=args.input_topic,
        hdfs_base_path=args.hdfs_path
    )
    
    if args.list:
        storage.list_stored_data()
    else:
        storage.buffer_size = args.buffer_size
        storage.consume_and_store()

if __name__ == "__main__":
    main()
