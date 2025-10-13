#!/usr/bin/env python3
"""
Pipeline Orchestrator
Manages and coordinates all components of the DDoS detection pipeline
"""

import subprocess
import time
import argparse
import signal
import sys
from datetime import datetime

class PipelineOrchestrator:
    def __init__(self):
        self.processes = {}
        self.running = False
        
        # Component configurations
        self.components = {
            'log_producer': {
                'script': 'scripts/log_producer.py',
                'args': ['--mode', 'normal', '--duration', '60'],
                'description': 'Network Log Producer'
            },
            'hdfs_storage': {
                'script': 'scripts/hdfs_storage.py',
                'args': [],
                'description': 'HDFS Storage Service'
            },
            'ddos_detector': {
                'script': 'scripts/ddos_detector.py',
                'args': [],
                'description': 'Rule-based DDoS Detector'
            },
            'spark_detector': {
                'script': 'scripts/spark_stream_detector.py',
                'args': ['--output-mode', 'kafka'],
                'description': 'Spark Streaming Detector'
            },
            'hbase_storage': {
                'script': 'scripts/hbase_anomaly_store.py',
                'args': [],
                'description': 'HBase Anomaly Storage'
            },
            'alert_monitor': {
                'script': 'scripts/alert_monitor.py',
                'args': [],
                'description': 'Alert Monitor'
            }
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print("\n⚠️ Received shutdown signal...")
        self.stop_all()
        sys.exit(0)

    def start_component(self, name, wait_time=2):
        """Start a single component"""
        if name not in self.components:
            print(f"❌ Unknown component: {name}")
            return False
        
        component = self.components[name]
        
        try:
            print(f"🚀 Starting {component['description']}...")
            
            # Build command
            cmd = ['python', component['script']] + component['args']
            
            # Start process
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            self.processes[name] = {
                'process': process,
                'start_time': datetime.now(),
                'description': component['description']
            }
            
            print(f"✅ {component['description']} started (PID: {process.pid})")
            
            # Wait before starting next component
            time.sleep(wait_time)
            
            return True
            
        except Exception as e:
            print(f"❌ Error starting {name}: {e}")
            return False

    def stop_component(self, name):
        """Stop a single component"""
        if name not in self.processes:
            print(f"⚠️ Component {name} is not running")
            return
        
        proc_info = self.processes[name]
        process = proc_info['process']
        
        print(f"⏹️ Stopping {proc_info['description']}...")
        
        try:
            # Try graceful shutdown first
            process.terminate()
            
            # Wait up to 5 seconds
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill if needed
                process.kill()
                process.wait()
            
            uptime = datetime.now() - proc_info['start_time']
            print(f"✅ {proc_info['description']} stopped (Uptime: {str(uptime).split('.')[0]})")
            
            del self.processes[name]
            
        except Exception as e:
            print(f"❌ Error stopping {name}: {e}")

    def check_component_status(self, name):
        """Check if a component is running"""
        if name not in self.processes:
            return False
        
        process = self.processes[name]['process']
        return process.poll() is None

    def monitor_components(self):
        """Monitor running components"""
        while self.running:
            print(f"\n{'='*60}")
            print(f"📊 Pipeline Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")
            
            for name, proc_info in self.processes.items():
                process = proc_info['process']
                status = "🟢 Running" if process.poll() is None else "🔴 Stopped"
                uptime = datetime.now() - proc_info['start_time']
                
                print(f"{status} | {proc_info['description']}")
                print(f"           PID: {process.pid} | Uptime: {str(uptime).split('.')[0]}")
            
            print(f"{'='*60}\n")
            
            # Check if any process has stopped unexpectedly
            for name in list(self.processes.keys()):
                if not self.check_component_status(name):
                    print(f"⚠️ {self.processes[name]['description']} has stopped unexpectedly!")
                    del self.processes[name]
            
            # Wait before next check
            time.sleep(30)

    def start_full_pipeline(self, components_to_start=None):
        """Start the complete pipeline"""
        print("="*60)
        print("🚀 Starting DDoS Detection Pipeline")
        print("="*60)
        
        # Default components to start
        if components_to_start is None:
            components_to_start = [
                'hdfs_storage',
                'ddos_detector',
                'hbase_storage',
                'alert_monitor',
                'log_producer'
            ]
        
        # Start each component with delay
        for component in components_to_start:
            if not self.start_component(component):
                print(f"⚠️ Failed to start {component}, continuing anyway...")
        
        print("\n" + "="*60)
        print(f"✅ Pipeline started with {len(self.processes)} components")
        print("="*60)
        
        self.running = True

    def start_streaming_pipeline(self):
        """Start streaming-focused pipeline"""
        print("="*60)
        print("🌊 Starting Streaming Pipeline")
        print("="*60)
        
        components = [
            'spark_detector',
            'hbase_storage',
            'alert_monitor',
            'log_producer'
        ]
        
        self.start_full_pipeline(components)

    def start_batch_pipeline(self):
        """Start batch processing pipeline"""
        print("="*60)
        print("📦 Starting Batch Pipeline")
        print("="*60)
        
        components = [
            'hdfs_storage',
            'log_producer'
        ]
        
        self.start_full_pipeline(components)

    def stop_all(self):
        """Stop all running components"""
        print("\n" + "="*60)
        print("⏹️ Stopping All Components")
        print("="*60)
        
        self.running = False
        
        # Stop in reverse order
        component_names = list(self.processes.keys())
        for name in reversed(component_names):
            self.stop_component(name)
        
        print("\n✅ All components stopped")

    def interactive_mode(self):
        """Interactive control interface"""
        print("\n" + "="*60)
        print("🎮 Interactive Pipeline Control")
        print("="*60)
        
        while True:
            print("\nCommands:")
            print("  1. Start full pipeline")
            print("  2. Start streaming pipeline")
            print("  3. Start batch pipeline")
            print("  4. Start specific component")
            print("  5. Stop specific component")
            print("  6. Show status")
            print("  7. Stop all and exit")
            
            choice = input("\nEnter command (1-7): ").strip()
            
            if choice == '1':
                self.start_full_pipeline()
                print("\n⏱️ Monitoring for 60 seconds...")
                time.sleep(60)
                self.monitor_components()
                
            elif choice == '2':
                self.start_streaming_pipeline()
                print("\n⏱️ Monitoring for 60 seconds...")
                time.sleep(60)
                
            elif choice == '3':
                self.start_batch_pipeline()
                print("\n⏱️ Monitoring for 60 seconds...")
                time.sleep(60)
                
            elif choice == '4':
                print("\nAvailable components:")
                for i, name in enumerate(self.components.keys(), 1):
                    print(f"  {i}. {name}")
                comp_idx = input("Select component: ").strip()
                try:
                    comp_name = list(self.components.keys())[int(comp_idx) - 1]
                    self.start_component(comp_name)
                except (ValueError, IndexError):
                    print("❌ Invalid selection")
                    
            elif choice == '5':
                if not self.processes:
                    print("⚠️ No components running")
                else:
                    print("\nRunning components:")
                    for i, name in enumerate(self.processes.keys(), 1):
                        print(f"  {i}. {name}")
                    comp_idx = input("Select component to stop: ").strip()
                    try:
                        comp_name = list(self.processes.keys())[int(comp_idx) - 1]
                        self.stop_component(comp_name)
                    except (ValueError, IndexError):
                        print("❌ Invalid selection")
                        
            elif choice == '6':
                if not self.processes:
                    print("\n⚠️ No components running")
                else:
                    self.monitor_components()
                    break
                    
            elif choice == '7':
                self.stop_all()
                break
                
            else:
                print("❌ Invalid command")

def main():
    parser = argparse.ArgumentParser(description='DDoS Detection Pipeline Orchestrator')
    parser.add_argument('--mode', choices=['full', 'streaming', 'batch', 'interactive'],
                       default='interactive',
                       help='Pipeline mode to run')
    parser.add_argument('--duration', type=int, default=300,
                       help='Duration to run pipeline in seconds (for non-interactive modes)')
    
    args = parser.parse_args()
    
    orchestrator = PipelineOrchestrator()
    
    try:
        if args.mode == 'interactive':
            orchestrator.interactive_mode()
            
        elif args.mode == 'full':
            orchestrator.start_full_pipeline()
            print(f"\n⏱️ Running for {args.duration} seconds...")
            time.sleep(args.duration)
            orchestrator.stop_all()
            
        elif args.mode == 'streaming':
            orchestrator.start_streaming_pipeline()
            print(f"\n⏱️ Running for {args.duration} seconds...")
            time.sleep(args.duration)
            orchestrator.stop_all()
            
        elif args.mode == 'batch':
            orchestrator.start_batch_pipeline()
            print(f"\n⏱️ Running for {args.duration} seconds...")
            time.sleep(args.duration)
            orchestrator.stop_all()
            
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
        orchestrator.stop_all()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        orchestrator.stop_all()

if __name__ == "__main__":
    main()
