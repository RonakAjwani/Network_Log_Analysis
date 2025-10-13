docker-compose up -d
docker ps
docker-compose restart
docker logs kafka
docker logs spark-master
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
docker exec -it namenode hdfs dfs -ls -R /ddos
echo "list" | docker exec -i hbase hbase shell
# DDoS Detection With The Hadoop Ecosystem

## Why This Project Matters

This repository shows how a DDoS detection workflow can be implemented end to end with the six staple components of the Hadoop ecosystem. Real-time traffic is ingested with Kafka, analysed with Python detectors, stored in HDFS and HBase, and surfaced through dashboards. Everything here can be run locally with Docker and a Python virtualenv.

## Component Matrix

| Component | Role | How It Is Used |
|-----------|------|----------------|
| Apache Kafka (7.4.0) | Streaming backbone | `log_producer.py` publishes traffic to `network-logs`; detectors consume and emit alerts to `ddos-alerts` |
| Apache Zookeeper (7.4.0) | Coordination | Coordinates Kafka and exposes demo coordination helpers in `zookeeper_coordinator.py` |
| Apache HDFS (3.2.1) | Distributed storage | Raw traffic and ML artefacts are archived by `hdfs_storage.py` |
| Apache Spark (3.4.1) | Distributed compute | Containers run for completeness; batch analytics via `bigdata_analyzer.py`; Spark Streaming script is optional |
| Apache HBase (1.4) | NoSQL alert store | `hbase_anomaly_store.py` persists anomalies; `hbase_analytics.py` queries threat intel |
| Mahout-style ML (scikit-learn + Spark MLlib) | Machine learning | `mahout_ml_detector.py` and `mahout_distributed_trainer.py` supply clustering and ensemble models |

## Prerequisites

- Docker Desktop running
- Python 3.8+
- At least 8 GB RAM available to Docker

> ℹ️ Spark Streaming on Windows requires Java 17 and winutils; the main run commands below omit `spark_stream_detector.py` for a smoother experience. The Spark containers still start so the ecosystem is complete.

## 1. Start The Hadoop Stack

```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
docker compose up -d
Start-Sleep -Seconds 30
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## 2. Prepare Python Environment

```powershell
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

> Run `venv\Scripts\Activate.ps1` in every PowerShell window before executing project scripts.

## 3. Kafka Topics (first run only)

```powershell
docker exec kafka kafka-topics --create --topic network-logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic ddos-alerts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic spark-alerts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## 4. Launch The Detection Pipeline

Open **four** PowerShell terminals (plus optional extras) and run the commands in the order shown.

### Terminal 1 – Traffic Generator
```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
venv\Scripts\Activate.ps1
python scripts\log_producer.py --mode normal --duration 5 --ddos-prob 0.7
```
Generates realistic traffic with a 70% probability of DDoS behaviour.

### Terminal 2 – Rule & ML Detector
```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
venv\Scripts\Activate.ps1
python scripts\ddos_detector.py
```
Consumes Kafka logs, applies heuristics and ML models, emits alerts to `ddos-alerts` and stores summaries.

### Terminal 3 – Mahout ML Detector
```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
venv\Scripts\Activate.ps1
python scripts\mahout_ml_detector.py
```
Runs clustering and ensemble models for anomaly classification.

### Terminal 4 – Real-time Alert Monitor
```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
venv\Scripts\Activate.ps1
python scripts\alert_monitor.py
```
Streams alert output from Kafka to the console for quick visibility.

### Optional Terminals

**Dashboard:**
```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
venv\Scripts\Activate.ps1
python scripts\dashboard.py
```

**HDFS Archiving:**
```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
venv\Scripts\Activate.ps1
python scripts\hdfs_storage.py
```

**HBase Storage:**
```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
venv\Scripts\Activate.ps1
python scripts\hbase_anomaly_store.py
```

**Threat Intelligence Analytics:**
```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
venv\Scripts\Activate.ps1
python scripts\hbase_analytics.py --action report
```

**Distributed Training / Batch Analytics:**
```powershell
python scripts\mahout_distributed_trainer.py
python scripts\bigdata_analyzer.py --analysis all
```

## 5. Web Interfaces

| Component | URL |
|-----------|-----|
| Kafka UI | http://localhost:8080 |
| HDFS NameNode | http://localhost:9870 |
| HBase Master | http://localhost:16010 |
| Spark Master | http://localhost:8081 |
| Spark Worker | http://localhost:8082 |
| Streamlit Dashboard (optional) | http://localhost:8501 |

## 6. Verification Snippets

### Kafka Messages
```powershell
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic network-logs --from-beginning --max-messages 10
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ddos-alerts --from-beginning --max-messages 10
```

### HDFS Listings
```powershell
docker exec namenode hdfs dfs -ls /
docker exec namenode hdfs dfs -ls /ddos_detection/raw_logs
docker exec namenode hdfs dfs -ls /ddos_detection/models
```

### HBase Tables
```powershell
docker exec -it hbase hbase shell
```
Inside the shell:
```
list
scan 'ddos_alerts', {LIMIT => 10}
scan 'ip_statistics', {LIMIT => 5}
exit
```

## 7. Stopping Everything

```powershell
# stop Python processes with Ctrl+C in each terminal
docker compose down
```

Add `-v` to prune volumes if you want a clean slate: `docker compose down -v`.

## Repository Layout

```

    log_producer.py            # Kafka traffic generator
    ddos_detector.py           # Rule/ML detection pipeline
    mahout_ml_detector.py      # Mahout-style ML detector
    mahout_distributed_trainer.py
    bigdata_analyzer.py        # Spark SQL analytics
    hdfs_storage.py            # Persist logs to HDFS
    hbase_anomaly_store.py     # Persist anomalies to HBase
    hbase_analytics.py         # Threat intelligence reporting
    alert_monitor.py           # Kafka alert monitor
    dashboard.py               # Real-time dashboard
    zookeeper_coordinator.py   # Coordination demos
config/
    pipeline_config.json       # Tunable thresholds and endpoints
    hadoop.env                 # Docker Hadoop environment settings
```

Comprehensive documentation lives in:
- `HADOOP_ECOSYSTEM_GUIDE.md`
- `HADOOP_INTEGRATION_SUMMARY.md`
- `HOW_TO_RUN.md`

## Troubleshooting Cheatsheet

- **Docker health:** `docker ps`, `docker logs <container>`
- **Kafka topics:** `docker exec kafka kafka-topics --list --bootstrap-server localhost:9092`
- **Missing Python deps:** ensure the venv is activated and rerun `pip install -r requirements.txt`
- **Spark Streaming on Windows:** install Java 17 and set `HADOOP_HOME` if you want to use `spark_stream_detector.py`

Enjoy exploring the complete Hadoop ecosystem for DDoS detection! 🎯

