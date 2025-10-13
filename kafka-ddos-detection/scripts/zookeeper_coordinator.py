#!/usr/bin/env python3
"""
Zookeeper Coordinator Service
Provides distributed coordination, configuration management, and leader election
for the DDoS detection pipeline components
"""

import json
import time
from datetime import datetime
from kazoo.client import KazooClient
from kazoo.recipe.election import Election
from kazoo.recipe.watchers import DataWatch, ChildrenWatch
from kazoo.exceptions import NodeExistsError, NoNodeError
import argparse
import sys

class ZookeeperCoordinator:
    """
    Centralized Zookeeper coordinator for distributed DDoS detection system
    Handles:
    - Leader election for components
    - Configuration management
    - Service discovery
    - Component health monitoring
    - Distributed locks
    """
    
    def __init__(self, zk_hosts='localhost:2181', namespace='ddos-detection'):
        self.zk_hosts = zk_hosts
        self.namespace = namespace
        self.zk = None
        self.election = None
        self.is_leader = False
        self.component_name = None
        
        # Zookeeper paths
        self.base_path = f"/{namespace}"
        self.config_path = f"{self.base_path}/config"
        self.components_path = f"{self.base_path}/components"
        self.leaders_path = f"{self.base_path}/leaders"
        self.locks_path = f"{self.base_path}/locks"
        self.health_path = f"{self.base_path}/health"
        
        # Statistics
        self.start_time = datetime.now()
        
    def connect(self):
        """Establish connection to Zookeeper"""
        try:
            print(f"🔌 Connecting to Zookeeper at {self.zk_hosts}...")
            self.zk = KazooClient(hosts=self.zk_hosts, timeout=10.0)
            self.zk.start()
            
            # Initialize directory structure
            self._initialize_structure()
            
            print(f"✅ Connected to Zookeeper successfully")
            return True
            
        except Exception as e:
            print(f"❌ Failed to connect to Zookeeper: {e}")
            return False
    
    def _initialize_structure(self):
        """Initialize Zookeeper directory structure"""
        paths = [
            self.base_path,
            self.config_path,
            self.components_path,
            self.leaders_path,
            self.locks_path,
            self.health_path
        ]
        
        for path in paths:
            try:
                self.zk.ensure_path(path)
            except NodeExistsError:
                pass
        
        print(f"✅ Zookeeper structure initialized at {self.base_path}")
    
    def register_component(self, component_name, component_info=None):
        """Register a component in Zookeeper"""
        self.component_name = component_name
        component_path = f"{self.components_path}/{component_name}"
        
        if component_info is None:
            component_info = {
                'name': component_name,
                'start_time': datetime.now().isoformat(),
                'host': 'localhost',
                'status': 'running'
            }
        
        try:
            # Create ephemeral node (auto-deleted when connection lost)
            self.zk.create(
                component_path,
                json.dumps(component_info).encode('utf-8'),
                ephemeral=True,
                makepath=True
            )
            print(f"✅ Registered component: {component_name}")
            
            # Start heartbeat
            self._start_heartbeat(component_name)
            
            return True
            
        except NodeExistsError:
            print(f"⚠️ Component {component_name} already registered")
            return False
        except Exception as e:
            print(f"❌ Error registering component: {e}")
            return False
    
    def _start_heartbeat(self, component_name):
        """Start heartbeat updates for health monitoring"""
        health_node = f"{self.health_path}/{component_name}"
        
        def update_heartbeat():
            while self.zk.connected:
                try:
                    health_data = {
                        'component': component_name,
                        'timestamp': datetime.now().isoformat(),
                        'status': 'healthy',
                        'is_leader': self.is_leader
                    }
                    
                    if self.zk.exists(health_node):
                        self.zk.set(health_node, json.dumps(health_data).encode('utf-8'))
                    else:
                        self.zk.create(health_node, json.dumps(health_data).encode('utf-8'), 
                                     ephemeral=True, makepath=True)
                    
                except Exception as e:
                    print(f"⚠️ Heartbeat error: {e}")
                
                time.sleep(5)  # Heartbeat every 5 seconds
        
        # Start heartbeat in background (in production, use proper threading)
        import threading
        heartbeat_thread = threading.Thread(target=update_heartbeat, daemon=True)
        heartbeat_thread.start()
    
    def elect_leader(self, component_name, on_elected=None, on_lost=None):
        """Participate in leader election for a component"""
        leader_path = f"{self.leaders_path}/{component_name}"
        self.zk.ensure_path(leader_path)
        
        def leader_func():
            """Called when this instance becomes leader"""
            self.is_leader = True
            print(f"👑 Elected as LEADER for {component_name}")
            if on_elected:
                on_elected()
            
            # Keep leadership while connected
            while self.zk.connected and self.is_leader:
                time.sleep(1)
        
        try:
            election = Election(self.zk, leader_path)
            election.run(leader_func)
            
        except Exception as e:
            self.is_leader = False
            print(f"❌ Lost leadership for {component_name}: {e}")
            if on_lost:
                on_lost()
    
    def set_config(self, key, value):
        """Set a configuration value in Zookeeper"""
        config_node = f"{self.config_path}/{key}"
        
        try:
            config_data = json.dumps(value).encode('utf-8')
            
            if self.zk.exists(config_node):
                self.zk.set(config_node, config_data)
            else:
                self.zk.create(config_node, config_data, makepath=True)
            
            print(f"✅ Set config {key} = {value}")
            return True
            
        except Exception as e:
            print(f"❌ Error setting config: {e}")
            return False
    
    def get_config(self, key, default=None):
        """Get a configuration value from Zookeeper"""
        config_node = f"{self.config_path}/{key}"
        
        try:
            if self.zk.exists(config_node):
                data, stat = self.zk.get(config_node)
                return json.loads(data.decode('utf-8'))
            else:
                return default
                
        except Exception as e:
            print(f"❌ Error getting config: {e}")
            return default
    
    def watch_config(self, key, callback):
        """Watch for configuration changes"""
        config_node = f"{self.config_path}/{key}"
        
        @DataWatch(self.zk, config_node)
        def config_watcher(data, stat):
            if data is not None:
                value = json.loads(data.decode('utf-8'))
                print(f"📢 Config changed: {key} = {value}")
                callback(key, value)
        
        print(f"👀 Watching config: {key}")
    
    def list_active_components(self):
        """List all currently active components"""
        try:
            components = self.zk.get_children(self.components_path)
            
            print(f"\n📋 Active Components ({len(components)}):")
            for component in components:
                comp_path = f"{self.components_path}/{component}"
                data, stat = self.zk.get(comp_path)
                info = json.loads(data.decode('utf-8'))
                print(f"   🟢 {component}")
                print(f"      Start time: {info.get('start_time', 'unknown')}")
                print(f"      Status: {info.get('status', 'unknown')}")
            
            return components
            
        except Exception as e:
            print(f"❌ Error listing components: {e}")
            return []
    
    def get_component_health(self, component_name=None):
        """Get health status of components"""
        try:
            if component_name:
                # Get specific component health
                health_node = f"{self.health_path}/{component_name}"
                if self.zk.exists(health_node):
                    data, stat = self.zk.get(health_node)
                    return json.loads(data.decode('utf-8'))
                else:
                    return None
            else:
                # Get all component health
                components = self.zk.get_children(self.health_path)
                health_status = {}
                
                for component in components:
                    health_node = f"{self.health_path}/{component}"
                    data, stat = self.zk.get(health_node)
                    health_status[component] = json.loads(data.decode('utf-8'))
                
                return health_status
                
        except Exception as e:
            print(f"❌ Error getting health status: {e}")
            return None
    
    def acquire_lock(self, lock_name, timeout=10):
        """Acquire a distributed lock"""
        lock_path = f"{self.locks_path}/{lock_name}"
        
        try:
            lock = self.zk.Lock(lock_path, self.component_name or "unknown")
            acquired = lock.acquire(timeout=timeout)
            
            if acquired:
                print(f"🔒 Acquired lock: {lock_name}")
                return lock
            else:
                print(f"⏳ Failed to acquire lock: {lock_name} (timeout)")
                return None
                
        except Exception as e:
            print(f"❌ Error acquiring lock: {e}")
            return None
    
    def release_lock(self, lock):
        """Release a distributed lock"""
        try:
            lock.release()
            print(f"🔓 Released lock")
            return True
        except Exception as e:
            print(f"❌ Error releasing lock: {e}")
            return False
    
    def watch_components(self, callback):
        """Watch for component registration/deregistration"""
        @ChildrenWatch(self.zk, self.components_path)
        def component_watcher(children):
            print(f"📢 Component list changed: {len(children)} active")
            callback(children)
        
        print(f"👀 Watching component registrations")
    
    def get_leader_for_component(self, component_name):
        """Get the current leader for a component type"""
        leader_path = f"{self.leaders_path}/{component_name}"
        
        try:
            if self.zk.exists(leader_path):
                children = self.zk.get_children(leader_path)
                if children:
                    # First child is the leader
                    leader_id = sorted(children)[0]
                    return leader_id
            return None
            
        except Exception as e:
            print(f"❌ Error getting leader: {e}")
            return None
    
    def shutdown(self):
        """Shutdown Zookeeper connection"""
        print(f"\n⏹️ Shutting down Zookeeper coordinator...")
        
        if self.zk:
            self.zk.stop()
            self.zk.close()
        
        uptime = datetime.now() - self.start_time
        print(f"✅ Zookeeper coordinator stopped (Uptime: {str(uptime).split('.')[0]})")

def demo_coordinator():
    """Demonstration of Zookeeper coordinator features"""
    import random
    
    coordinator = ZookeeperCoordinator()
    
    if not coordinator.connect():
        print("❌ Failed to connect to Zookeeper")
        return
    
    # Register this component
    component_name = f"demo-component-{random.randint(1000, 9999)}"
    coordinator.register_component(component_name, {
        'name': component_name,
        'type': 'demo',
        'start_time': datetime.now().isoformat()
    })
    
    # Set some configuration
    print("\n📝 Setting configuration values...")
    coordinator.set_config('detection_threshold', 50)
    coordinator.set_config('window_size', 300)
    coordinator.set_config('enable_ml', True)
    
    # Read configuration
    print("\n📖 Reading configuration...")
    threshold = coordinator.get_config('detection_threshold')
    window = coordinator.get_config('window_size')
    enable_ml = coordinator.get_config('enable_ml')
    print(f"   Threshold: {threshold}")
    print(f"   Window: {window}")
    print(f"   ML Enabled: {enable_ml}")
    
    # List active components
    coordinator.list_active_components()
    
    # Get health status
    print("\n💚 Component Health:")
    health = coordinator.get_component_health()
    for comp, status in health.items():
        print(f"   {comp}: {status['status']} (Leader: {status.get('is_leader', False)})")
    
    # Test distributed lock
    print("\n🔒 Testing distributed lock...")
    lock = coordinator.acquire_lock('demo-lock', timeout=5)
    if lock:
        print("   Performing critical operation...")
        time.sleep(2)
        coordinator.release_lock(lock)
    
    # Keep running
    print(f"\n✅ Demo running. Press Ctrl+C to stop...")
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n⚠️ Stopping demo...")
    finally:
        coordinator.shutdown()

def main():
    parser = argparse.ArgumentParser(description='Zookeeper Coordinator Service')
    parser.add_argument('--zk-hosts', default='localhost:2181',
                       help='Zookeeper hosts (comma-separated)')
    parser.add_argument('--namespace', default='ddos-detection',
                       help='Zookeeper namespace for this application')
    parser.add_argument('--component', type=str,
                       help='Component name to register')
    parser.add_argument('--demo', action='store_true',
                       help='Run demonstration mode')
    parser.add_argument('--list-components', action='store_true',
                       help='List active components and exit')
    parser.add_argument('--health', action='store_true',
                       help='Show component health and exit')
    parser.add_argument('--set-config', nargs=2, metavar=('KEY', 'VALUE'),
                       help='Set configuration value')
    parser.add_argument('--get-config', type=str, metavar='KEY',
                       help='Get configuration value')
    
    args = parser.parse_args()
    
    coordinator = ZookeeperCoordinator(
        zk_hosts=args.zk_hosts,
        namespace=args.namespace
    )
    
    if not coordinator.connect():
        sys.exit(1)
    
    try:
        if args.demo:
            demo_coordinator()
            
        elif args.list_components:
            coordinator.list_active_components()
            
        elif args.health:
            health = coordinator.get_component_health()
            print("\n💚 Component Health Status:")
            for comp, status in health.items():
                emoji = "🟢" if status['status'] == 'healthy' else "🔴"
                leader = "👑" if status.get('is_leader') else "  "
                print(f"{emoji} {leader} {comp}: {status['status']} ({status['timestamp']})")
            
        elif args.set_config:
            key, value = args.set_config
            # Try to parse value as JSON
            try:
                value = json.loads(value)
            except:
                pass  # Keep as string
            coordinator.set_config(key, value)
            
        elif args.get_config:
            value = coordinator.get_config(args.get_config)
            print(f"{args.get_config} = {value}")
            
        elif args.component:
            # Register and keep running
            coordinator.register_component(args.component)
            print(f"✅ Component {args.component} registered. Press Ctrl+C to stop...")
            while True:
                time.sleep(1)
        else:
            print("⚠️ No action specified. Use --demo or see --help")
            
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
    finally:
        coordinator.shutdown()

if __name__ == "__main__":
    main()
