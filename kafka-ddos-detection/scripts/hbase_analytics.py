#!/usr/bin/env python3
"""
HBase Analytics and Threat Intelligence Reporter
Advanced querying, analytics, and reporting on HBase-stored alert data
"""

import json
from datetime import datetime, timedelta
from collections import defaultdict
import happybase
import argparse
from tabulate import tabulate

class HBaseAnalytics:
    """
    Advanced analytics and reporting on HBase data
    Provides threat intelligence, historical analysis, and trend detection
    """
    
    def __init__(self, hbase_host='localhost', hbase_port=9090):
        self.hbase_host = hbase_host
        self.hbase_port = hbase_port
        self.connection = None
        self.alerts_table = None
        self.ip_stats_table = None
        
        self._connect()
    
    def _connect(self):
        """Connect to HBase"""
        try:
            print(f"🔌 Connecting to HBase at {self.hbase_host}:{self.hbase_port}...")
            self.connection = happybase.Connection(
                host=self.hbase_host,
                port=self.hbase_port,
                timeout=30000
            )
            
            # Get table references
            if b'ddos_alerts' in self.connection.tables():
                self.alerts_table = self.connection.table('ddos_alerts')
            else:
                print("⚠️ Alert table not found. Run hbase_anomaly_store first.")
            
            if b'ip_statistics' in self.connection.tables():
                self.ip_stats_table = self.connection.table('ip_statistics')
            else:
                print("⚠️ IP statistics table not found.")
            
            print("✅ Connected to HBase successfully\n")
            
        except Exception as e:
            print(f"❌ Failed to connect to HBase: {e}")
            raise
    
    def get_alert_count(self):
        """Get total number of alerts"""
        count = 0
        for key, data in self.alerts_table.scan(limit=10000):
            count += 1
        return count
    
    def query_recent_alerts(self, limit=50):
        """Query most recent alerts"""
        print("\n" + "="*80)
        print("🚨 RECENT ALERTS")
        print("="*80)
        
        alerts = []
        
        # Scan in reverse (most recent first)
        for key, data in self.alerts_table.scan(limit=limit, reverse=True):
            try:
                alert = {
                    'timestamp': data.get(b'alert:timestamp', b'').decode('utf-8'),
                    'source_ip': data.get(b'alert:source_ip', b'').decode('utf-8'),
                    'severity': data.get(b'alert:severity', b'').decode('utf-8'),
                    'method': data.get(b'alert:detection_method', b'').decode('utf-8'),
                }
                alerts.append(alert)
            except:
                pass
        
        if alerts:
            # Format as table
            table_data = [
                [a['timestamp'][:19], a['source_ip'], a['severity'], a['method']]
                for a in alerts
            ]
            
            print(tabulate(
                table_data,
                headers=['Timestamp', 'Source IP', 'Severity', 'Detection Method'],
                tablefmt='grid'
            ))
            print(f"\nTotal: {len(alerts)} alerts")
        else:
            print("ℹ️ No alerts found")
        
        return alerts
    
    def analyze_alert_trends(self):
        """Analyze alert trends over time"""
        print("\n" + "="*80)
        print("📈 ALERT TREND ANALYSIS")
        print("="*80)
        
        alerts_by_hour = defaultdict(int)
        alerts_by_severity = defaultdict(int)
        alerts_by_method = defaultdict(int)
        
        for key, data in self.alerts_table.scan():
            try:
                timestamp = data.get(b'alert:timestamp', b'').decode('utf-8')
                severity = data.get(b'alert:severity', b'').decode('utf-8')
                method = data.get(b'alert:detection_method', b'').decode('utf-8')
                
                # Parse timestamp and group by hour
                if timestamp:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    hour_key = dt.strftime('%Y-%m-%d %H:00')
                    alerts_by_hour[hour_key] += 1
                
                if severity:
                    alerts_by_severity[severity] += 1
                
                if method:
                    alerts_by_method[method] += 1
                    
            except:
                pass
        
        # Print severity distribution
        print("\n1. Alerts by Severity:")
        severity_data = [[s, c] for s, c in sorted(alerts_by_severity.items())]
        print(tabulate(severity_data, headers=['Severity', 'Count'], tablefmt='grid'))
        
        # Print detection method distribution
        print("\n2. Alerts by Detection Method:")
        method_data = [[m, c] for m, c in sorted(alerts_by_method.items(), key=lambda x: x[1], reverse=True)]
        print(tabulate(method_data, headers=['Method', 'Count'], tablefmt='grid'))
        
        # Print hourly trends
        print("\n3. Alerts by Hour (last 24 hours):")
        sorted_hours = sorted(alerts_by_hour.items(), reverse=True)[:24]
        hour_data = [[h, c] for h, c in sorted_hours]
        print(tabulate(hour_data, headers=['Hour', 'Count'], tablefmt='grid'))
        
        return {
            'by_severity': dict(alerts_by_severity),
            'by_method': dict(alerts_by_method),
            'by_hour': dict(alerts_by_hour)
        }
    
    def get_top_offenders(self, limit=20):
        """Get top offending IPs"""
        print("\n" + "="*80)
        print(f"🎯 TOP {limit} OFFENDING IPs")
        print("="*80)
        
        ip_data = []
        
        if self.ip_stats_table:
            for key, data in self.ip_stats_table.scan():
                try:
                    ip = data.get(b'info:ip_address', b'').decode('utf-8')
                    alert_count = int(data.get(b'stats:alert_count', b'0'))
                    last_alert = data.get(b'info:last_alert_time', b'').decode('utf-8')
                    last_severity = data.get(b'stats:last_severity', b'').decode('utf-8')
                    
                    if alert_count > 0:
                        ip_data.append({
                            'ip': ip,
                            'alerts': alert_count,
                            'last_alert': last_alert[:19] if last_alert else 'N/A',
                            'severity': last_severity
                        })
                except:
                    pass
            
            # Sort by alert count
            ip_data.sort(key=lambda x: x['alerts'], reverse=True)
            
            if ip_data:
                table_data = [
                    [i+1, ip['ip'], ip['alerts'], ip['last_alert'], ip['severity']]
                    for i, ip in enumerate(ip_data[:limit])
                ]
                
                print(tabulate(
                    table_data,
                    headers=['Rank', 'IP Address', 'Total Alerts', 'Last Alert', 'Last Severity'],
                    tablefmt='grid'
                ))
            else:
                print("ℹ️ No IP statistics found")
        else:
            print("⚠️ IP statistics table not available")
        
        return ip_data[:limit]
    
    def query_ip_history(self, ip_address):
        """Get complete alert history for an IP"""
        print("\n" + "="*80)
        print(f"📋 ALERT HISTORY FOR IP: {ip_address}")
        print("="*80)
        
        alerts = []
        
        # Scan all alerts for this IP
        for key, data in self.alerts_table.scan():
            try:
                source_ip = data.get(b'alert:source_ip', b'').decode('utf-8')
                
                if source_ip == ip_address:
                    alert = {
                        'timestamp': data.get(b'alert:timestamp', b'').decode('utf-8'),
                        'severity': data.get(b'alert:severity', b'').decode('utf-8'),
                        'method': data.get(b'alert:detection_method', b'').decode('utf-8'),
                        'request_rate': data.get(b'features:request_rate', b'').decode('utf-8'),
                        'error_rate': data.get(b'features:error_rate', b'').decode('utf-8'),
                    }
                    alerts.append(alert)
            except:
                pass
        
        if alerts:
            # Sort by timestamp
            alerts.sort(key=lambda x: x['timestamp'], reverse=True)
            
            print(f"\nFound {len(alerts)} alerts for {ip_address}")
            print("\nAlert Timeline:")
            
            table_data = [
                [
                    a['timestamp'][:19],
                    a['severity'],
                    a['method'],
                    a.get('request_rate', 'N/A'),
                    a.get('error_rate', 'N/A')
                ]
                for a in alerts[:50]  # Show last 50
            ]
            
            print(tabulate(
                table_data,
                headers=['Timestamp', 'Severity', 'Method', 'Req/Min', 'Error Rate'],
                tablefmt='grid'
            ))
            
            # Get IP statistics
            if self.ip_stats_table:
                print("\n📊 IP Statistics:")
                rowkey = ip_address.replace('.', '_').encode('utf-8')
                stats_data = self.ip_stats_table.row(rowkey)
                
                if stats_data:
                    print(f"   Total Alerts: {stats_data.get(b'stats:alert_count', b'0').decode('utf-8')}")
                    print(f"   Last Alert: {stats_data.get(b'info:last_alert_time', b'N/A').decode('utf-8')[:19]}")
                    print(f"   Last Severity: {stats_data.get(b'stats:last_severity', b'N/A').decode('utf-8')}")
        else:
            print(f"ℹ️ No alerts found for IP: {ip_address}")
        
        return alerts
    
    def generate_threat_report(self):
        """Generate comprehensive threat intelligence report"""
        print("\n" + "="*80)
        print("🛡️  COMPREHENSIVE THREAT INTELLIGENCE REPORT")
        print("="*80)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        # Get overall statistics
        print("\n1. OVERALL STATISTICS")
        print("-" * 80)
        
        total_alerts = self.get_alert_count()
        print(f"   Total Alerts: {total_alerts:,}")
        
        # Get unique IPs with alerts
        unique_ips = set()
        for key, data in self.alerts_table.scan():
            try:
                ip = data.get(b'alert:source_ip', b'').decode('utf-8')
                if ip:
                    unique_ips.add(ip)
            except:
                pass
        
        print(f"   Unique Threat Sources: {len(unique_ips)}")
        print(f"   Average Alerts per IP: {total_alerts/max(len(unique_ips), 1):.1f}")
        
        # Analyze trends
        trends = self.analyze_alert_trends()
        
        # Get top offenders
        top_offenders = self.get_top_offenders(limit=10)
        
        # Severity breakdown
        print("\n2. THREAT SEVERITY BREAKDOWN")
        print("-" * 80)
        
        severity_counts = trends['by_severity']
        total = sum(severity_counts.values())
        
        for severity in ['HIGH', 'MEDIUM', 'LOW', 'CRITICAL']:
            if severity in severity_counts:
                count = severity_counts[severity]
                pct = (count / total * 100) if total > 0 else 0
                print(f"   {severity:10s}: {count:5d} ({pct:5.1f}%)")
        
        # Detection method effectiveness
        print("\n3. DETECTION METHOD EFFECTIVENESS")
        print("-" * 80)
        
        method_counts = trends['by_method']
        for method, count in sorted(method_counts.items(), key=lambda x: x[1], reverse=True):
            pct = (count / total * 100) if total > 0 else 0
            print(f"   {method:25s}: {count:5d} ({pct:5.1f}%)")
        
        # Critical IPs
        print("\n4. CRITICAL THREAT SOURCES")
        print("-" * 80)
        
        critical_ips = [ip for ip in top_offenders if ip['alerts'] >= 5]
        
        if critical_ips:
            print(f"   {len(critical_ips)} IPs with 5+ alerts (require immediate attention)")
            for ip in critical_ips[:5]:
                print(f"   • {ip['ip']:15s} - {ip['alerts']} alerts (Last: {ip['severity']})")
        else:
            print("   No critical threat sources identified")
        
        # Recommendations
        print("\n5. RECOMMENDATIONS")
        print("-" * 80)
        
        if len(critical_ips) > 0:
            print("   ⚠️  IMMEDIATE ACTION REQUIRED:")
            print(f"      • Block or rate-limit {len(critical_ips)} high-risk IPs")
            print("      • Review firewall rules for suspicious traffic patterns")
        
        if severity_counts.get('HIGH', 0) + severity_counts.get('CRITICAL', 0) > total * 0.3:
            print("   ⚠️  HIGH SEVERITY RATE:")
            print("      • Consider increasing detection thresholds")
            print("      • Review and optimize server capacity")
        
        if method_counts.get('spark_streaming', 0) < method_counts.get('rule_based', 0) * 0.5:
            print("   ℹ️  ML DETECTION USAGE:")
            print("      • Consider enabling more ML-based detection methods")
            print("      • Review and retrain models with recent data")
        
        print("\n" + "="*80)
        print("✅ REPORT COMPLETE")
        print("="*80)
        
        return {
            'total_alerts': total_alerts,
            'unique_ips': len(unique_ips),
            'trends': trends,
            'top_offenders': top_offenders,
            'critical_ips': critical_ips
        }
    
    def export_to_json(self, output_file='hbase_analytics_report.json'):
        """Export analytics to JSON file"""
        print(f"\n📄 Exporting analytics to {output_file}...")
        
        report = self.generate_threat_report()
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"✅ Report exported to {output_file}")
        
        return output_file
    
    def cleanup(self):
        """Close HBase connection"""
        if self.connection:
            self.connection.close()
            print("\n✅ HBase connection closed")

def main():
    parser = argparse.ArgumentParser(description='HBase Analytics and Threat Intelligence')
    parser.add_argument('--hbase-host', default='localhost',
                       help='HBase Thrift server host')
    parser.add_argument('--hbase-port', type=int, default=9090,
                       help='HBase Thrift server port')
    parser.add_argument('--action', choices=[
        'recent', 'trends', 'offenders', 'ip-history', 'report', 'export'
    ], default='report', help='Analysis action to perform')
    parser.add_argument('--ip', type=str,
                       help='IP address for ip-history action')
    parser.add_argument('--limit', type=int, default=20,
                       help='Limit for results')
    parser.add_argument('--output', type=str, default='hbase_analytics_report.json',
                       help='Output file for export action')
    
    args = parser.parse_args()
    
    try:
        analytics = HBaseAnalytics(
            hbase_host=args.hbase_host,
            hbase_port=args.hbase_port
        )
        
        if args.action == 'recent':
            analytics.query_recent_alerts(limit=args.limit)
            
        elif args.action == 'trends':
            analytics.analyze_alert_trends()
            
        elif args.action == 'offenders':
            analytics.get_top_offenders(limit=args.limit)
            
        elif args.action == 'ip-history':
            if not args.ip:
                print("❌ --ip parameter required for ip-history action")
            else:
                analytics.query_ip_history(args.ip)
                
        elif args.action == 'report':
            analytics.generate_threat_report()
            
        elif args.action == 'export':
            analytics.export_to_json(args.output)
        
        analytics.cleanup()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
