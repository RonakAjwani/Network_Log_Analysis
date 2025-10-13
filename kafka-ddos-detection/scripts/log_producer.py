#!/usr/bin/env python3
"""
Realistic Network Log Producer for DDoS Detection
Generates realistic network traffic patterns and DDoS attack simulations
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import argparse

class NetworkLogProducer:
    def __init__(self, kafka_server='localhost:9092', topic='network-logs'):
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_server],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8')
        )
        self.topic = topic
        
        # More realistic IP addresses (simulating different geographic regions)
        self.normal_ips = [
            # North America
            '72.14.192.101', '66.249.64.35', '8.8.8.8', '74.125.224.72',
            # Europe
            '185.70.40.35', '162.158.166.101', '31.13.64.35',
            # Asia
            '103.102.166.224', '202.75.33.59', '210.16.120.12',
            # Local network
            '192.168.1.10', '192.168.1.20', '10.0.0.5', '172.16.0.100'
        ]
        
        self.suspicious_ips = [
            # Known bad actor ranges
            '45.227.253.%d' % random.randint(1, 254),
            '185.220.101.%d' % random.randint(1, 254),
            '198.51.100.%d' % random.randint(1, 254),
            '203.0.113.%d' % random.randint(1, 254)
        ]
        
        # Realistic URLs with different endpoint types
        self.urls = {
            'static': ['/index.html', '/about.html', '/contact.html', '/css/style.css', '/js/app.js', 
                      '/images/logo.png', '/images/banner.jpg', '/favicon.ico'],
            'api': ['/api/v1/users', '/api/v1/products', '/api/v1/orders', '/api/v1/auth/login',
                   '/api/v1/search', '/api/v1/analytics'],
            'dynamic': ['/login.php', '/dashboard', '/admin/panel', '/user/profile', '/checkout',
                       '/search?q=products', '/category/electronics'],
            'heavy': ['/api/v1/reports/generate', '/export/data', '/admin/logs', '/analytics/dashboard']
        }
        
        # Realistic user agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 Safari/604.1',
            'Mozilla/5.0 (iPad; CPU OS 16_0 like Mac OS X) AppleWebKit/605.1.15 Safari/604.1',
            'Mozilla/5.0 (Android 13; Mobile) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36'
        ]
        
        self.bot_agents = [
            'python-requests/2.31.0', 'curl/7.68.0', 'Wget/1.20.3',
            'bot/1.0', 'scanner/2.0', 'automated-tool/1.0'
        ]
        
        # HTTP methods and status codes with realistic distributions
        self.methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']
        self.status_codes = [200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503]

    def generate_normal_log(self):
        """Generate a realistic normal network log entry"""
        # Choose endpoint type based on realistic distribution
        endpoint_type = random.choices(
            ['static', 'api', 'dynamic', 'heavy'],
            weights=[50, 30, 15, 5]
        )[0]
        
        url = random.choice(self.urls[endpoint_type])
        
        # Method distribution based on endpoint type
        if endpoint_type == 'static':
            method = 'GET'
            status = random.choices([200, 304, 404], weights=[85, 10, 5])[0]
            response_time = round(random.uniform(0.05, 0.3), 3)
            bytes_sent = random.randint(500, 5000)
        elif endpoint_type == 'api':
            method = random.choice(['GET', 'POST', 'PUT', 'DELETE'])
            status = random.choices([200, 201, 400, 401, 500], weights=[70, 15, 8, 5, 2])[0]
            response_time = round(random.uniform(0.1, 1.5), 3)
            bytes_sent = random.randint(200, 3000)
        elif endpoint_type == 'dynamic':
            method = random.choice(['GET', 'POST'])
            status = random.choices([200, 302, 401, 403], weights=[75, 15, 7, 3])[0]
            response_time = round(random.uniform(0.2, 2.0), 3)
            bytes_sent = random.randint(1000, 8000)
        else:  # heavy
            method = random.choice(['GET', 'POST'])
            status = random.choices([200, 500, 503], weights=[80, 15, 5])[0]
            response_time = round(random.uniform(1.0, 5.0), 3)
            bytes_sent = random.randint(5000, 50000)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'source_ip': random.choice(self.normal_ips),
            'method': method,
            'url': url,
            'status_code': status,
            'bytes_sent': bytes_sent,
            'response_time': response_time,
            'user_agent': random.choice(self.user_agents)
        }

    def generate_ddos_log(self, intensity='medium'):
        """Generate a realistic DDoS attack log entry with varying intensity"""
        # Generate or pick a suspicious IP
        if random.random() < 0.3:
            source_ip = '45.227.%d.%d' % (random.randint(1, 255), random.randint(1, 255))
        else:
            source_ip = random.choice(self.suspicious_ips)
        
        # DDoS characteristics based on intensity
        if intensity == 'low':
            status = random.choices([200, 503, 429], weights=[70, 20, 10])[0]
            response_time = round(random.uniform(1.0, 3.0), 3)
            bytes_sent = random.randint(100, 1000)
        elif intensity == 'high':
            status = random.choices([503, 429, 500], weights=[50, 30, 20])[0]
            response_time = round(random.uniform(5.0, 15.0), 3)
            bytes_sent = random.randint(50, 300)
        else:  # medium
            status = random.choices([200, 503, 429], weights=[50, 35, 15])[0]
            response_time = round(random.uniform(2.0, 8.0), 3)
            bytes_sent = random.randint(100, 500)
        
        # Target popular endpoints
        target_urls = ['/index.html', '/api/v1/login', '/login.php', '/api/v1/search']
        
        return {
            'timestamp': datetime.now().isoformat(),
            'source_ip': source_ip,
            'method': 'GET',  # DDoS usually uses GET
            'url': random.choice(target_urls),
            'status_code': status,
            'bytes_sent': bytes_sent,
            'response_time': response_time,
            'user_agent': random.choice(self.bot_agents)
        }

    def simulate_traffic(self, duration_minutes=5, ddos_probability=0.1):
        """Simulate realistic network traffic with varying patterns"""
        print(f"🚀 Starting realistic traffic simulation for {duration_minutes} minutes...")
        print(f"📊 DDoS probability: {ddos_probability*100}%")
        print(f"📡 Kafka topic: {self.topic}")
        print(f"⏱️  Simulating real-time traffic patterns...")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        log_count = 0
        ddos_count = 0
        
        try:
            while time.time() < end_time:
                # Calculate time-based traffic patterns (simulate daily patterns)
                elapsed = time.time() - start_time
                # Create traffic waves (peak and off-peak simulation)
                traffic_multiplier = 1 + 0.3 * abs(random.gauss(0, 1))
                
                # Burst mode: occasionally send multiple requests at once (realistic)
                burst_size = 1
                if random.random() < 0.1:  # 10% chance of burst
                    burst_size = random.randint(2, 5)
                
                for _ in range(burst_size):
                    # Decide if this should be a DDoS log
                    if random.random() < ddos_probability:
                        intensity = random.choice(['low', 'medium', 'high'])
                        log_entry = self.generate_ddos_log(intensity)
                        ddos_count += 1
                        icon = "🔴"
                    else:
                        log_entry = self.generate_normal_log()
                        icon = "🟢"
                    
                    # Send to Kafka
                    self.producer.send(
                        self.topic,
                        key=log_entry['source_ip'],
                        value=log_entry
                    )
                    
                    log_count += 1
                    
                    # Print real-time progress with more detail
                    if log_count % 50 == 0:
                        elapsed_mins = int((time.time() - start_time) / 60)
                        rate = log_count / max((time.time() - start_time), 1)
                        print(f"{icon} [{elapsed_mins}m] {log_count} logs | {rate:.1f} req/s | {ddos_count} suspicious ({(ddos_count/log_count)*100:.1f}%)")
                
                # Variable delay for realistic traffic (some fast, some slow)
                base_delay = 0.05
                jitter = random.uniform(0, 0.15)
                time.sleep(base_delay + jitter)
                
        except KeyboardInterrupt:
            print("\n⚠️ Stopping traffic simulation...")
        
        finally:
            self.producer.flush()
            self.producer.close()
            elapsed_mins = (time.time() - start_time) / 60
            avg_rate = log_count / max((time.time() - start_time), 1)
            print(f"\n{'='*60}")
            print(f"✅ Simulation Complete")
            print(f"   Duration: {elapsed_mins:.1f} minutes")
            print(f"   Total Logs: {log_count:,}")
            print(f"   Suspicious: {ddos_count} ({(ddos_count/log_count)*100:.1f}%)")
            print(f"   Avg Rate: {avg_rate:.1f} requests/second")
            print(f"{'='*60}")

    def simulate_ddos_attack(self, attack_ip=None, duration_seconds=30):
        """Simulate a realistic DDoS attack with gradual buildup and intensity variation"""
        if attack_ip is None:
            attack_ip = f'45.227.{random.randint(1,255)}.{random.randint(1,255)}'
        
        print(f"{'='*60}")
        print(f"🔴 DDoS ATTACK SIMULATION STARTED")
        print(f"{'='*60}")
        print(f"   Attack IP: {attack_ip}")
        print(f"   Duration: {duration_seconds} seconds")
        print(f"   Pattern: Gradual buildup → Peak → Sustained")
        print(f"{'='*60}\n")
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        attack_count = 0
        phase = "buildup"
        
        try:
            while time.time() < end_time:
                elapsed = time.time() - start_time
                progress = elapsed / duration_seconds
                
                # Determine attack phase and intensity
                if progress < 0.2:  # First 20% - Buildup phase
                    phase = "🟡 BUILDUP"
                    delay = random.uniform(0.1, 0.3)
                    intensity = 'low'
                elif progress < 0.6:  # 20-60% - Peak attack
                    phase = "🔴 PEAK"
                    delay = random.uniform(0.01, 0.05)
                    intensity = 'high'
                elif progress < 0.9:  # 60-90% - Sustained attack
                    phase = "🟠 SUSTAINED"
                    delay = random.uniform(0.03, 0.1)
                    intensity = 'medium'
                else:  # Final 10% - Decline
                    phase = "🟢 DECLINE"
                    delay = random.uniform(0.1, 0.2)
                    intensity = 'low'
                
                # Generate attack traffic with some variation
                num_requests = random.randint(1, 3) if phase == "🔴 PEAK" else 1
                
                for _ in range(num_requests):
                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'source_ip': attack_ip,
                        'method': 'GET',
                        'url': random.choice(['/index.html', '/api/v1/login', '/login.php']),
                        'status_code': random.choice([503, 429, 500, 200]),
                        'bytes_sent': random.randint(50, 200),
                        'response_time': round(random.uniform(5.0, 15.0), 3),
                        'user_agent': random.choice(self.bot_agents)
                    }
                    
                    self.producer.send(
                        self.topic,
                        key=log_entry['source_ip'],
                        value=log_entry
                    )
                    
                    attack_count += 1
                
                # Real-time progress updates
                if attack_count % 100 == 0:
                    rate = attack_count / max(elapsed, 1)
                    remaining = int(end_time - time.time())
                    print(f"{phase} | {attack_count:,} requests | {rate:.0f} req/s | {remaining}s remaining")
                
                time.sleep(delay)
                
        except KeyboardInterrupt:
            print("\n⚠️ Attack simulation interrupted...")
        
        finally:
            self.producer.flush()
            self.producer.close()
            total_time = time.time() - start_time
            avg_rate = attack_count / max(total_time, 1)
            print(f"\n{'='*60}")
            print(f"🔴 DDoS ATTACK SIMULATION COMPLETE")
            print(f"{'='*60}")
            print(f"   Total Requests: {attack_count:,}")
            print(f"   Duration: {total_time:.1f} seconds")
            print(f"   Average Rate: {avg_rate:.0f} requests/second")
            print(f"   Peak Rate: ~{avg_rate*3:.0f} requests/second")
            print(f"{'='*60}")

def main():
    parser = argparse.ArgumentParser(description='Network Log Producer for DDoS Detection')
    parser.add_argument('--mode', choices=['normal', 'attack'], default='normal',
                       help='Simulation mode: normal traffic or DDoS attack')
    parser.add_argument('--duration', type=int, default=5,
                       help='Duration in minutes for normal mode, seconds for attack mode')
    parser.add_argument('--ddos-prob', type=float, default=0.1,
                       help='Probability of DDoS logs in normal mode (0.0-1.0)')
    parser.add_argument('--kafka-server', default='localhost:9092',
                       help='Kafka bootstrap server')
    parser.add_argument('--topic', default='network-logs',
                       help='Kafka topic name')
    
    args = parser.parse_args()
    
    producer = NetworkLogProducer(args.kafka_server, args.topic)
    
    if args.mode == 'normal':
        producer.simulate_traffic(args.duration, args.ddos_prob)
    elif args.mode == 'attack':
        producer.simulate_ddos_attack(duration_seconds=args.duration)

if __name__ == "__main__":
    main()