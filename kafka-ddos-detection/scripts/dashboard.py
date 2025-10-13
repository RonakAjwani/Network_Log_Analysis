#!/usr/bin/env python3
"""
Real-Time DDoS Detection Dashboard
Streamlit-based web interface for visualizing the pipeline
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict, deque
import threading
import time

# Page configuration
st.set_page_config(
    page_title="DDoS Detection Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
        padding: 1rem;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        animation: pulse 2s ease-in-out infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.8; }
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.8rem;
        border-left: 5px solid #1f77b4;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        transition: transform 0.2s;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    .alert-box {
        background: linear-gradient(135deg, #fee 0%, #fdd 100%);
        padding: 1rem;
        border-radius: 0.8rem;
        border-left: 5px solid #d32f2f;
        margin: 0.5rem 0;
        animation: slideIn 0.3s ease-out;
    }
    @keyframes slideIn {
        from { transform: translateX(-20px); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    .status-indicator {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        margin-right: 5px;
        animation: blink 2s ease-in-out infinite;
    }
    @keyframes blink {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    .green { background-color: #4caf50; }
    .red { background-color: #f44336; }
    .yellow { background-color: #ff9800; }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Smooth transitions */
    .stMetric {
        transition: all 0.3s ease;
    }
</style>
""", unsafe_allow_html=True)

class DashboardData:
    """Manages data collection and storage for the dashboard"""
    
    def __init__(self):
        self.logs = deque(maxlen=1000)
        self.alerts = deque(maxlen=100)
        self.ip_stats = defaultdict(lambda: {
            'count': 0,
            'errors': 0,
            'total_response_time': 0,
            'last_seen': datetime.now()
        })
        self.timeline_data = deque(maxlen=100)
        self.running = False
        self.log_count = 0
        self.alert_count = 0
        self.consumer_logs = None
        self.consumer_alerts = None
        
    def start_consumers(self, kafka_server='localhost:9092'):
        """Start Kafka consumers in background threads"""
        try:
            self.consumer_logs = KafkaConsumer(
                'network-logs',
                bootstrap_servers=[kafka_server],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                consumer_timeout_ms=1000
            )
            
            self.consumer_alerts = KafkaConsumer(
                'ddos-alerts',
                bootstrap_servers=[kafka_server],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                consumer_timeout_ms=1000
            )
            
            self.running = True
            
            # Start background threads
            threading.Thread(target=self._consume_logs, daemon=True).start()
            threading.Thread(target=self._consume_alerts, daemon=True).start()
            
            return True
        except Exception as e:
            st.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def _consume_logs(self):
        """Background thread to consume network logs"""
        while self.running:
            try:
                for message in self.consumer_logs:
                    log = message.value
                    self.logs.append(log)
                    self.log_count += 1
                    
                    # Update IP statistics
                    ip = log.get('source_ip', 'unknown')
                    self.ip_stats[ip]['count'] += 1
                    if log.get('status_code', 200) >= 400:
                        self.ip_stats[ip]['errors'] += 1
                    self.ip_stats[ip]['total_response_time'] += log.get('response_time', 0)
                    self.ip_stats[ip]['last_seen'] = datetime.now()
                    
                    # Update timeline
                    now = datetime.now()
                    self.timeline_data.append({
                        'timestamp': now,
                        'logs': 1,
                        'alerts': 0
                    })
                    
            except Exception as e:
                if self.running:
                    time.sleep(1)
    
    def _consume_alerts(self):
        """Background thread to consume alerts"""
        while self.running:
            try:
                for message in self.consumer_alerts:
                    alert = message.value
                    self.alerts.append(alert)
                    self.alert_count += 1
                    
                    # Update timeline
                    now = datetime.now()
                    self.timeline_data.append({
                        'timestamp': now,
                        'logs': 0,
                        'alerts': 1
                    })
                    
            except Exception as e:
                if self.running:
                    time.sleep(1)
    
    def stop(self):
        """Stop consumers"""
        self.running = False
        if self.consumer_logs:
            self.consumer_logs.close()
        if self.consumer_alerts:
            self.consumer_alerts.close()

# Initialize session state
if 'data' not in st.session_state:
    st.session_state.data = DashboardData()
    st.session_state.connected = False

def main():
    # Header with live status
    col_header1, col_header2 = st.columns([4, 1])
    with col_header1:
        st.markdown('<h1 class="main-header">🛡️ Real-Time DDoS Detection Dashboard</h1>', unsafe_allow_html=True)
    with col_header2:
        if st.session_state.connected:
            st.markdown('<div style="text-align: right; padding-top: 2rem;"><span class="status-indicator green"></span><b>LIVE</b></div>', unsafe_allow_html=True)
    
    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Configuration")
        kafka_server = st.text_input("Kafka Server", value="localhost:9092")
        
        if not st.session_state.connected:
            if st.button("🚀 Connect to Pipeline", type="primary", use_container_width=True):
                with st.spinner("Connecting to Kafka..."):
                    if st.session_state.data.start_consumers(kafka_server):
                        st.session_state.connected = True
                        st.success("✅ Connected!")
                        st.rerun()
        else:
            st.success("✅ Connected to Pipeline")
            if st.button("🛑 Disconnect", use_container_width=True):
                st.session_state.data.stop()
                st.session_state.connected = False
                st.rerun()
        
        st.divider()
        st.header("📊 Dashboard Controls")
        
        # Auto-refresh toggle
        auto_refresh = st.checkbox("🔄 Auto-Refresh", value=True, help="Automatically refresh dashboard")
        
        if auto_refresh:
            refresh_rate = st.slider("Refresh Rate (seconds)", 1.0, 10.0, 3.0, 0.5)
        else:
            st.info("⏸️ Auto-refresh paused. Use manual refresh below.")
            if st.button("🔄 Refresh Now", use_container_width=True):
                st.rerun()
        
        show_charts = st.checkbox("Show Analytics Charts", value=True)
        show_logs = st.checkbox("Show Packet Capture", value=False, help="Wireshark-style logs (toggle to reduce clutter)")
        max_alerts_display = st.slider("Max Alerts to Display", 3, 10, 3)
        
        st.divider()
        st.markdown("### 🔍 Pipeline Status")
        components = [
            ("Kafka Broker", True),
            ("HDFS Storage", True),
            ("Spark Streaming", True),
            ("HBase Database", True)
        ]
        for component, status in components:
            color = "green" if status else "red"
            st.markdown(f'<span class="status-indicator {color}"></span> {component}', unsafe_allow_html=True)
        
        st.divider()
        st.markdown("### � Quick Stats")
        if st.session_state.connected:
            data = st.session_state.data
            st.metric("Total Requests", f"{data.log_count:,}")
            st.metric("Active IPs", len(data.ip_stats))
            if data.log_count > 0:
                threat_level = "🔴 HIGH" if (data.alert_count / data.log_count) > 0.1 else "🟡 MEDIUM" if (data.alert_count / data.log_count) > 0.05 else "🟢 LOW"
                st.markdown(f"**Threat Level:** {threat_level}")
    
    if not st.session_state.connected:
        st.info("👈 Click 'Connect to Pipeline' in the sidebar to start monitoring")
        st.stop()
    
    data = st.session_state.data
    
    # Key Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        delta_color = "normal" if len(data.logs) > 0 else "off"
        st.metric(
            label="📨 Total Logs Processed",
            value=f"{data.log_count:,}",
            delta=f"+{len(data.logs)} in buffer",
            delta_color=delta_color
        )
    
    with col2:
        attack_rate = (data.alert_count / max(data.log_count, 1)) * 100 if data.log_count > 0 else 0
        delta_sign = "⚠️" if data.alert_count > 0 else "✓"
        st.metric(
            label="🚨 Total Alerts",
            value=f"{data.alert_count}",
            delta=f"{delta_sign} {len(data.alerts)} recent",
            delta_color="inverse" if data.alert_count > 0 else "off"
        )
    
    with col3:
        unique_ips = len(data.ip_stats)
        st.metric(
            label="🌐 Unique IPs",
            value=f"{unique_ips}",
            delta="Active sources"
        )
    
    with col4:
        emoji = "🔴" if attack_rate > 10 else "🟡" if attack_rate > 5 else "🟢"
        st.metric(
            label=f"{emoji} Attack Rate",
            value=f"{attack_rate:.2f}%",
            delta=f"{'Critical' if attack_rate > 10 else 'Elevated' if attack_rate > 5 else 'Normal'}",
            delta_color="inverse" if attack_rate > 5 else "off"
        )
    
    st.divider()
    
    # Analytics Section - Organized in Tabs
    if show_charts and len(data.timeline_data) > 0:
        st.subheader("📈 Real-Time Analytics & Insights")
        
        # Create tabs for better organization
        tab1, tab2, tab3 = st.tabs(["📊 Traffic Overview", "🔍 Detailed Analysis", "🎯 Threat Intelligence"])
        
        with tab1:
            # First Row - Timeline and Request Rate
            col1, col2 = st.columns(2)
            
            with col1:
                # Timeline chart
                timeline_df = pd.DataFrame(list(data.timeline_data))
                if not timeline_df.empty:
                    timeline_df = timeline_df.groupby('timestamp').sum().reset_index()
                    
                    fig_timeline = go.Figure()
                    fig_timeline.add_trace(go.Scatter(
                        x=timeline_df['timestamp'],
                        y=timeline_df['logs'],
                        name='Network Traffic',
                        mode='lines',
                        line=dict(color='#1f77b4', width=2),
                        fill='tozeroy',
                        fillcolor='rgba(31, 119, 180, 0.3)'
                    ))
                    fig_timeline.add_trace(go.Scatter(
                        x=timeline_df['timestamp'],
                        y=timeline_df['alerts'],
                        name='DDoS Alerts',
                        mode='lines+markers',
                        line=dict(color='#d32f2f', width=3),
                        marker=dict(size=10, symbol='x')
                    ))
                    
                    fig_timeline.update_layout(
                        title="Traffic & Alert Timeline",
                        xaxis_title="Time",
                        yaxis_title="Count",
                        hovermode='x unified',
                        height=350,
                        template="plotly_dark",
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0.1)',
                        font=dict(size=12),
                        showlegend=True,
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1
                        )
                    )
                    st.plotly_chart(fig_timeline, use_container_width=True)
            
            with col2:
                # Request rate over time (moving average)
                if len(data.logs) > 10:
                    recent_logs = list(data.logs)[-100:]
                    log_df = pd.DataFrame(recent_logs)
                    log_df['timestamp'] = pd.to_datetime(log_df['timestamp'])
                    log_df = log_df.set_index('timestamp')
                    
                    # Calculate requests per second
                    request_rate = log_df.resample('5S').size()
                    
                    fig_rate = go.Figure()
                    fig_rate.add_trace(go.Scatter(
                        x=request_rate.index,
                        y=request_rate.values,
                        mode='lines+markers',
                        name='Request Rate',
                        line=dict(color='#ff7f0e', width=2),
                        marker=dict(size=6),
                        fill='tozeroy',
                        fillcolor='rgba(255, 127, 14, 0.3)'
                    ))
                    
                    fig_rate.update_layout(
                        title="Request Rate (per 5 seconds)",
                        xaxis_title="Time",
                        yaxis_title="Requests/5s",
                        height=350,
                        template="plotly_dark",
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0.1)',
                        font=dict(size=12)
                    )
                    st.plotly_chart(fig_rate, use_container_width=True)
        
        with tab2:
            # Detailed Analysis Tab
            col1, col2 = st.columns(2)
            
            with col1:
                # HTTP Status Code Distribution (Pie Chart)
                if len(data.logs) > 0:
                    recent_logs = list(data.logs)[-200:]
                    status_counts = pd.Series([log.get('status_code', 0) for log in recent_logs]).value_counts()
                    
                    # Color mapping for status codes
                    colors_map = {
                        200: '#4caf50', 201: '#66bb6a', 204: '#81c784',
                        301: '#42a5f5', 302: '#64b5f6',
                        400: '#ff9800', 401: '#ffa726', 403: '#ffb74d', 404: '#ffc107',
                        500: '#f44336', 502: '#ef5350', 503: '#e53935', 429: '#d32f2f'
                    }
                    
                    colors = [colors_map.get(code, '#9e9e9e') for code in status_counts.index]
                    
                    fig_status = go.Figure(data=[go.Pie(
                        labels=[f"HTTP {code}" for code in status_counts.index],
                        values=status_counts.values,
                        hole=0.4,
                        marker=dict(colors=colors),
                        textinfo='label+percent',
                        textposition='auto'
                    )])
                    
                    fig_status.update_layout(
                        title="HTTP Status Code Distribution",
                        height=350,
                        template="plotly_dark",
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(size=12),
                        showlegend=False
                    )
                    st.plotly_chart(fig_status, use_container_width=True)
            
            with col2:
                # Response Time Distribution (Histogram)
                if len(data.logs) > 10:
                    recent_logs = list(data.logs)[-200:]
                    response_times = [log.get('response_time', 0) for log in recent_logs]
                    
                    fig_response = go.Figure(data=[go.Histogram(
                        x=response_times,
                        nbinsx=20,
                        marker_color='#9c27b0',
                        opacity=0.75,
                        name='Response Time'
                    )])
                    
                    fig_response.update_layout(
                        title="Response Time Distribution",
                        xaxis_title="Response Time (seconds)",
                        yaxis_title="Frequency",
                        height=350,
                        template="plotly_dark",
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0.1)',
                        font=dict(size=12),
                        bargap=0.1
                    )
                    st.plotly_chart(fig_response, use_container_width=True)
            
            # HTTP Method Distribution (Full Width)
            if len(data.logs) > 0:
                recent_logs = list(data.logs)[-200:]
                methods = [log.get('method', 'UNKNOWN') for log in recent_logs]
                method_counts = pd.Series(methods).value_counts()
                
                method_colors = {
                    'GET': '#2196f3', 'POST': '#4caf50', 'PUT': '#ff9800',
                    'DELETE': '#f44336', 'PATCH': '#9c27b0'
                }
                colors = [method_colors.get(m, '#9e9e9e') for m in method_counts.index]
                
                fig_methods = go.Figure(data=[go.Bar(
                    x=method_counts.index,
                    y=method_counts.values,
                    marker_color=colors,
                    text=method_counts.values,
                    textposition='outside'
                )])
                
                fig_methods.update_layout(
                    title="HTTP Method Distribution",
                    xaxis_title="HTTP Method",
                    yaxis_title="Count",
                    height=300,
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0.1)',
                    font=dict(size=12)
                )
                st.plotly_chart(fig_methods, use_container_width=True)
        
        with tab3:
            # Threat Intelligence Tab
            # Top IPs Bar Chart (Enhanced)
            if data.ip_stats:
                top_ips = sorted(
                    data.ip_stats.items(),
                    key=lambda x: x[1]['count'],
                    reverse=True
                )[:10]
                
                ip_df = pd.DataFrame([
                    {
                        'IP': ip, 
                        'Requests': stats['count'],
                        'Error_Rate': (stats['errors'] / max(stats['count'], 1)) * 100
                    }
                    for ip, stats in top_ips
                ])
                
                fig_ips = go.Figure()
                fig_ips.add_trace(go.Bar(
                    y=ip_df['IP'],
                    x=ip_df['Requests'],
                    orientation='h',
                    marker=dict(
                        color=ip_df['Error_Rate'],
                        colorscale='RdYlGn_r',
                        showscale=True,
                        colorbar=dict(title="Error %")
                    ),
                    text=ip_df['Requests'],
                    textposition='outside',
                    hovertemplate='<b>%{y}</b><br>Requests: %{x}<br>Error Rate: %{marker.color:.1f}%<extra></extra>'
                ))
                
                fig_ips.update_layout(
                    title="Top 10 Active IPs (Color: Error Rate)",
                    xaxis_title="Request Count",
                    yaxis_title="IP Address",
                    height=400,
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0.1)',
                    font=dict(size=12)
                )
                st.plotly_chart(fig_ips, use_container_width=True)
    
    st.divider()
    
    # Recent Alerts Section
    if data.alerts:
        st.subheader("🚨 Recent Security Alerts")
        
        for alert in list(data.alerts)[-max_alerts_display:]:
            with st.container():
                # Alert header
                col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
                
                with col1:
                    timestamp = alert.get('timestamp', 'N/A')
                    if isinstance(timestamp, str) and 'T' in timestamp:
                        timestamp = timestamp.split('T')[1].split('.')[0]
                    st.markdown(f"**⏰ {timestamp}**")
                
                with col2:
                    st.markdown(f"**🌐 IP:** `{alert.get('source_ip', 'Unknown')}`")
                
                with col3:
                    alerts_list = alert.get('alerts', [])
                    if alerts_list:
                        severity = alerts_list[0].get('severity', 'MEDIUM')
                        severity_colors = {'HIGH': '🔴', 'MEDIUM': '🟡', 'LOW': '🟢'}
                        st.markdown(f"{severity_colors.get(severity, '⚪')} **{severity} Priority**")
                
                with col4:
                    if st.button("📊", key=f"expand_{alert.get('timestamp', id(alert))}"):
                        st.session_state[f"expanded_{id(alert)}"] = not st.session_state.get(f"expanded_{id(alert)}", False)
                
                # Alert details (expandable)
                if st.session_state.get(f"expanded_{id(alert)}", False):
                    features = alert.get('features', {})
                    col_a, col_b, col_c, col_d = st.columns(4)
                    
                    with col_a:
                        rate = features.get('request_rate', 0)
                        st.metric("Request Rate", f"{rate:.1f}", "req/min", delta_color="inverse")
                    with col_b:
                        err_rate = features.get('error_rate', 0)
                        st.metric("Error Rate", f"{err_rate:.1%}", delta_color="inverse")
                    with col_c:
                        resp_time = features.get('avg_response_time', 0)
                        st.metric("Avg Response", f"{resp_time:.2f}s", delta_color="inverse")
                    with col_d:
                        diversity = features.get('url_diversity', 0)
                        st.metric("URL Diversity", diversity)
                    
                    st.markdown("**⚠️ Triggered Rules:**")
                    for a in alerts_list:
                        st.markdown(f"- `{a.get('type', 'Unknown')}`: {a.get('message', 'N/A')}")
                
                st.divider()
    
    # Recent Logs Section - Wireshark Style
    if show_logs and data.logs:
        st.subheader("📋 Network Packet Capture (Wireshark-Style View)")
        
        # Wireshark-style filter bar
        col_filter1, col_filter2, col_filter3 = st.columns([2, 2, 1])
        with col_filter1:
            filter_ip = st.text_input("🔍 Filter by IP", placeholder="e.g., 192.168.1.10", key="filter_ip")
        with col_filter2:
            filter_status = st.selectbox("Filter by Status", ["All", "2xx Success", "4xx Client Error", "5xx Server Error"])
        with col_filter3:
            log_limit = st.selectbox("Show", [10, 20, 50], index=0)
        
        logs_df = pd.DataFrame(list(data.logs)[-log_limit:])
        
        if not logs_df.empty:
            # Apply filters
            if filter_ip:
                logs_df = logs_df[logs_df['source_ip'].str.contains(filter_ip, case=False)]
            
            if filter_status != "All":
                if filter_status == "2xx Success":
                    logs_df = logs_df[logs_df['status_code'].between(200, 299)]
                elif filter_status == "4xx Client Error":
                    logs_df = logs_df[logs_df['status_code'].between(400, 499)]
                elif filter_status == "5xx Server Error":
                    logs_df = logs_df[logs_df['status_code'].between(500, 599)]
            
            # Add packet number (Wireshark style)
            logs_df.insert(0, 'No.', range(1, len(logs_df) + 1))
            
            # Format timestamp (Wireshark style - relative time)
            if 'timestamp' in logs_df.columns:
                logs_df['Time'] = logs_df['timestamp'].apply(lambda x: x.split('T')[1].split('.')[0] if 'T' in str(x) else str(x))
            
            # Create Protocol column (HTTP/HTTPS simulation)
            logs_df['Protocol'] = 'HTTP'
            
            # Create Info column (Wireshark-style summary)
            logs_df['Info'] = logs_df.apply(
                lambda row: f"{row['method']} {row['url']} → {row['status_code']} ({row['bytes_sent']} bytes, {row['response_time']}s)",
                axis=1
            )
            
            # Reorder columns for Wireshark-like appearance
            display_columns = ['No.', 'Time', 'source_ip', 'Protocol', 'method', 'url', 'status_code', 'response_time', 'bytes_sent', 'Info']
            display_columns = [col for col in display_columns if col in logs_df.columns]
            display_df = logs_df[display_columns].copy()
            
            # Rename columns to Wireshark style
            display_df.columns = ['No.', 'Time', 'Source IP', 'Protocol', 'Method', 'Request URI', 'Status', 'Response (s)', 'Length (bytes)', 'Info']
            
            # Function to color rows based on status code (Wireshark style)
            def highlight_row(row):
                status = row['Status']
                if status >= 500:
                    return ['background-color: #5d0000; color: white'] * len(row)
                elif status >= 400:
                    return ['background-color: #5d3900; color: white'] * len(row)
                elif status >= 300:
                    return ['background-color: #003d5d; color: white'] * len(row)
                elif status >= 200 and status < 300:
                    return ['background-color: #003d00; color: #90ee90'] * len(row)
                return [''] * len(row)
            
            # Apply Wireshark-style coloring
            styled_df = display_df.style.apply(highlight_row, axis=1)
            
            # Display with fixed header
            st.markdown("""
            <style>
                .wireshark-table {
                    font-family: 'Courier New', monospace;
                    font-size: 12px;
                }
                .wireshark-header {
                    background-color: #2d2d2d;
                    color: #00ff00;
                    font-weight: bold;
                    padding: 10px;
                    border-radius: 5px;
                    margin-bottom: 10px;
                }
            </style>
            """, unsafe_allow_html=True)
            
            st.markdown('<div class="wireshark-header">📡 LIVE PACKET CAPTURE - DDoS Detection System</div>', unsafe_allow_html=True)
            
            st.dataframe(
                styled_df,
                use_container_width=True,
                height=400,
                hide_index=True
            )
            
            # Wireshark-style statistics
            col_stat1, col_stat2, col_stat3, col_stat4, col_stat5 = st.columns(5)
            with col_stat1:
                st.metric("📦 Packets", len(display_df))
            with col_stat2:
                total_bytes = display_df['Length (bytes)'].sum()
                st.metric("📊 Total Bytes", f"{total_bytes:,}")
            with col_stat3:
                avg_response = display_df['Response (s)'].mean()
                st.metric("⏱️ Avg Response", f"{avg_response:.3f}s")
            with col_stat4:
                protocols = display_df['Protocol'].value_counts()
                st.metric("🔌 Protocols", len(protocols))
            with col_stat5:
                unique_ips = display_df['Source IP'].nunique()
                st.metric("🌐 Unique IPs", unique_ips)
    
    # IP Statistics Section
    st.divider()
    st.subheader("🌐 Threat Intelligence & IP Analysis")
    
    if data.ip_stats:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🔴 Suspicious IPs (High Activity)**")
            # Find IPs with abnormal behavior
            suspicious = []
            for ip, stats in data.ip_stats.items():
                error_rate = (stats['errors'] / max(stats['count'], 1)) * 100
                avg_response = stats['total_response_time'] / max(stats['count'], 1)
                
                # Mark as suspicious if high error rate OR high request count OR slow responses
                if error_rate > 50 or stats['count'] > 100 or avg_response > 5:
                    threat_score = min(100, (error_rate * 0.4) + (min(stats['count'], 200) * 0.3) + (min(avg_response, 10) * 10))
                    suspicious.append({
                        'IP': ip,
                        'Requests': stats['count'],
                        'Error Rate': f"{error_rate:.1f}%",
                        'Avg Response': f"{avg_response:.2f}s",
                        'Threat Score': int(threat_score),
                        'Last Seen': stats['last_seen'].strftime('%H:%M:%S')
                    })
            
            if suspicious:
                suspicious_df = pd.DataFrame(sorted(suspicious, key=lambda x: x['Threat Score'], reverse=True)[:10])
                
                # Color code threat scores
                def color_threat(val):
                    if val >= 70:
                        return 'background-color: #d32f2f; color: white; font-weight: bold'
                    elif val >= 40:
                        return 'background-color: #ff9800; color: white'
                    else:
                        return 'background-color: #ffc107; color: black'
                
                styled_suspicious = suspicious_df.style.applymap(color_threat, subset=['Threat Score'])
                st.dataframe(styled_suspicious, use_container_width=True, hide_index=True)
            else:
                st.success("✅ No suspicious activity detected")
        
        with col2:
            st.markdown("**🟢 Normal Traffic IPs**")
            # Find IPs with normal behavior
            normal = []
            for ip, stats in data.ip_stats.items():
                error_rate = (stats['errors'] / max(stats['count'], 1)) * 100
                avg_response = stats['total_response_time'] / max(stats['count'], 1)
                
                # Mark as normal if low error rate AND reasonable request count
                if error_rate < 20 and stats['count'] < 100 and avg_response < 3:
                    normal.append({
                        'IP': ip,
                        'Requests': stats['count'],
                        'Error Rate': f"{error_rate:.1f}%",
                        'Avg Response': f"{avg_response:.2f}s",
                        'Last Seen': stats['last_seen'].strftime('%H:%M:%S')
                    })
            
            if normal:
                normal_df = pd.DataFrame(sorted(normal, key=lambda x: x['Requests'], reverse=True)[:10])
                st.dataframe(normal_df, use_container_width=True, hide_index=True)
            else:
                st.warning("⚠️ All traffic appears suspicious")
        
        # Threat Score Distribution
        st.markdown("**📊 Threat Score Distribution**")
        if data.ip_stats:
            threat_scores = []
            for ip, stats in data.ip_stats.items():
                error_rate = (stats['errors'] / max(stats['count'], 1)) * 100
                avg_response = stats['total_response_time'] / max(stats['count'], 1)
                threat_score = min(100, (error_rate * 0.4) + (min(stats['count'], 200) * 0.3) + (min(avg_response, 10) * 10))
                threat_scores.append(threat_score)
            
            fig_threat = go.Figure(data=[go.Histogram(
                x=threat_scores,
                nbinsx=20,
                marker_color='#f44336',
                opacity=0.75,
                name='Threat Score'
            )])
            
            # Add threshold lines
            fig_threat.add_vline(x=40, line_dash="dash", line_color="yellow", annotation_text="Medium Risk")
            fig_threat.add_vline(x=70, line_dash="dash", line_color="red", annotation_text="High Risk")
            
            fig_threat.update_layout(
                title="IP Threat Score Distribution",
                xaxis_title="Threat Score (0-100)",
                yaxis_title="Number of IPs",
                height=250,
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0.1)',
                font=dict(size=12),
                bargap=0.1
            )
            st.plotly_chart(fig_threat, use_container_width=True)
    
    # Network Traffic Summary Section
    st.divider()
    st.subheader("📡 Network Traffic Summary & Patterns")
    
    if len(data.logs) > 10:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Top URLs accessed
            st.markdown("**🔗 Most Accessed URLs**")
            recent_logs = list(data.logs)[-200:]
            urls = [log.get('url', 'unknown') for log in recent_logs]
            url_counts = pd.Series(urls).value_counts().head(8)
            
            for url, count in url_counts.items():
                percentage = (count / len(urls)) * 100
                st.markdown(f"`{url}` - {count} ({percentage:.1f}%)")
        
        with col2:
            # Traffic by time pattern
            st.markdown("**⏰ Traffic Intensity**")
            recent_logs = list(data.logs)[-100:]
            
            # Calculate metrics
            total_requests = len(recent_logs)
            total_bytes = sum(log.get('bytes_sent', 0) for log in recent_logs)
            avg_response = sum(log.get('response_time', 0) for log in recent_logs) / max(len(recent_logs), 1)
            
            st.metric("Total Requests", f"{total_requests}")
            st.metric("Total Data", f"{total_bytes:,} bytes")
            st.metric("Avg Response Time", f"{avg_response:.3f}s")
        
        with col3:
            # Attack indicators
            st.markdown("**⚠️ Attack Indicators**")
            
            # Calculate various attack indicators
            error_count = sum(1 for log in recent_logs if log.get('status_code', 200) >= 400)
            slow_requests = sum(1 for log in recent_logs if log.get('response_time', 0) > 5)
            bot_agents = sum(1 for log in recent_logs if 'bot' in log.get('user_agent', '').lower())
            
            st.markdown(f"🔴 High Error Rate: **{error_count}** ({(error_count/max(total_requests,1))*100:.1f}%)")
            st.markdown(f"🐌 Slow Responses: **{slow_requests}** ({(slow_requests/max(total_requests,1))*100:.1f}%)")
            st.markdown(f"🤖 Bot Traffic: **{bot_agents}** ({(bot_agents/max(total_requests,1))*100:.1f}%)")
            
            # Overall threat assessment
            threat_indicators = (error_count > total_requests * 0.3) + (slow_requests > total_requests * 0.2) + (bot_agents > total_requests * 0.1)
            if threat_indicators >= 2:
                st.error("🚨 HIGH THREAT LEVEL")
            elif threat_indicators == 1:
                st.warning("🟡 MEDIUM THREAT LEVEL")
            else:
                st.success("🟢 LOW THREAT LEVEL")
    
    # Auto-refresh logic (only if enabled)
    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()

if __name__ == "__main__":
    main()
