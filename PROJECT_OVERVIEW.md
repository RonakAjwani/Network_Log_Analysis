# Network Log Analysis Project Handbook

## 1. Project Synopsis
The repository delivers an end-to-end DDoS detection pipeline that showcases how every foundational Hadoop ecosystem component can be combined for network security analytics. Traffic is generated or ingested, streamed through Apache Kafka, inspected by both heuristic and machine-learning detectors, persisted in HDFS and HBase, and surfaced through dashboards and batch analytics. Docker Compose wraps the distributed stack (Kafka, Zookeeper, Hadoop HDFS, Spark, HBase) so the entire lab can run on a single workstation, while Python services glue the data flow together.

## 2. Technology Stack
| Layer | Technologies | Notes |
|-------|--------------|-------|
| Programming | Python 3.8+, Streamlit | Application services, dashboarding, orchestration |
| Streaming & Messaging | Apache Kafka 7.4 (Confluent), Kafka UI | Ingestion, alert distribution, observability |
| Coordination | Apache Zookeeper 7.4 | Coordination backbone, leader election, config |
| Storage | Hadoop HDFS 3.2 (NameNode/Datanode), Apache HBase 1.4 | Raw log archive, high-speed alert lookup |
| Distributed Compute | Apache Spark 3.4 (master/worker) | Structured Streaming, SQL analytics, MLlib training |
| Machine Learning | scikit-learn, Spark MLlib, joblib, numpy, pandas | Rule-based + ML detectors, distributed model training |
| Connectors | kafka-python, happybase, hdfs, kazoo | Service clients for Kafka, HBase, HDFS, Zookeeper |
| Visualization | Streamlit, plotly, matplotlib, seaborn | Live dashboard and reporting |
| Containerization | Docker Compose | Spins up the Hadoop ecosystem and supporting services |

Python package requirements are listed in `kafka-ddos-detection/requirements.txt`; Docker services are defined in `kafka-ddos-detection/docker-compose.yml`.

## 3. Runtime Topology (Docker)
Service containers launched by `docker compose up -d`:
- `zookeeper` (2181): Coordination for Kafka and demo leader election.
- `kafka` (9092): Primary message broker for logs and alerts.
- `kafka-ui` (8080): Web UI for interacting with Kafka topics.
- `namenode` (9870/9000) and `datanode` (9864): HDFS storage layer with volumes for persistence.
- `spark-master` (web UI 8081, RPC 7077) and `spark-worker` (web UI 8082): Spark cluster for streaming and batch jobs.
- `hbase` (16000/16010/16020/16030/9090): HBase master/region services exposed via Thrift for Python clients.

Volumes (`namenode-data`, `datanode-data`, `spark-data`) keep state across container restarts. Containers share the `ddos-network` bridge for internal communication.

## 4. End-to-End Data Flow
1. **Generation/Ingestion**: `scripts/log_producer.py` emits realistic HTTP traffic (benign and DDoS) into the `network-logs` Kafka topic.
2. **Real-time Detection**:
   - `scripts/ddos_detector.py` performs rule-based heuristics and forwards alerts to `ddos-alerts`.
   - `scripts/mahout_ml_detector.py` consumes the same stream, extracts advanced features, and produces ML-driven alerts to `ddos-alerts`.
   - `scripts/spark_stream_detector.py` (optional, requires local Spark dependencies) runs Structured Streaming against Kafka and publishes to `spark-alerts`.
3. **Storage Pipelines**:
   - `scripts/hdfs_storage.py` batches Kafka logs into partitioned HDFS paths (`/ddos/logs/raw/...`).
   - `scripts/hbase_anomaly_store.py` subscribes to alert topics (`ddos-alerts`, `spark-alerts`) and persists results to HBase tables (`ddos_alerts`, `ip_statistics`).
4. **Monitoring & Visualization**:
   - `scripts/alert_monitor.py` streams alerts to the console.
   - `scripts/dashboard.py` (Streamlit) provides a web dashboard with traffic, alert, and threat intelligence views.
5. **Analytics & Reporting**:
   - `scripts/bigdata_analyzer.py` runs Spark SQL workloads over HDFS data for trend analysis, suspicious IP detection, and threat intelligence.
   - `scripts/hbase_analytics.py` queries stored alerts to produce reports, trend charts, and JSON exports.
6. **Model Training**:
   - `scripts/mahout_distributed_trainer.py` delivers Spark-based (Mahout-style) distributed training, saving multiple models back to HDFS and local cache.
   - `scripts/ml_model_trainer.py` focuses on MLlib K-Means clustering with feature engineering and batch scoring support.

## 5. Application Components at a Glance
| Script | Role |
|--------|------|
| `log_producer.py` | Generates normal and attack traffic; supports bursts and dedicated attack simulations. |
| `ddos_detector.py` | Rule-based detection with request-rate, error-rate, and response-time heuristics; emits Kafka alerts. |
| `mahout_ml_detector.py` | Ensemble ML detector (K-Means, Isolation Forest, Random Forest); auto-trains and scores in real time. |
| `spark_stream_detector.py` | Spark Structured Streaming detector with windowed aggregations and optional Kafka/console sinks. |
| `hdfs_storage.py` | Buffers Kafka logs and writes JSON batches into partitioned HDFS directories. |
| `hbase_anomaly_store.py` | Captures alerts and stores them in HBase tables with IP statistics rollups. |
| `hbase_analytics.py` | Produces recent-alert views, offender rankings, trend charts, and threat reports from HBase. |
| `bigdata_analyzer.py` | Spark batch analytics: traffic patterns, suspicious IPs, attack windows, URL scans, response-time stats. |
| `mahout_distributed_trainer.py` | Distributed training for K-Means, Bisecting K-Means, Random Forest, Gradient Boosting (Spark MLlib). |
| `ml_model_trainer.py` | Focused Spark MLlib trainer with feature pipeline persistence and batch anomaly scoring. |
| `alert_monitor.py` | Console-based alert feed with severity annotations. |
| `dashboard.py` | Streamlit dashboard combining Kafka metrics, charts, Wireshark-style log view, and threat scoring. |
| `pipeline_orchestrator.py` | Optional launcher that starts/stops pipeline components and monitors their status. |
| `zookeeper_coordinator.py` | Demonstrates leader election, distributed locks, config distribution, and health tracking through Zookeeper. |

Model artefacts created by the ML detectors and trainers are stored in `kafka-ddos-detection/models/` (local) and `/ddos/models` (HDFS).

## 6. Configuration Surface
- `kafka-ddos-detection/config/pipeline_config.json`: Central configuration for Kafka topics, detection thresholds, Spark settings, HDFS paths, and component toggles. It also defines Mahout training parameters and Zookeeper coordination features.
- `kafka-ddos-detection/config/hadoop.env`: Core Hadoop, HDFS, and optional YARN environment variables consumed by the Docker images.

Tuning ideas:
- Adjust `detection.rule_based` thresholds to change alert sensitivity.
- Modify `spark.config` entries to scale Structured Streaming processing.
- Enable/disable components under the `components` section to tailor which services run.

## 7. Operational Recipes
### 7.1 Container Stack
```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
docker compose up -d
Start-Sleep -Seconds 30
```
Check health with `docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"`.

### 7.2 Python Environment
```powershell
cd C:\Projects\Network_Log_Analysis\kafka-ddos-detection
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
Activate the virtual environment in every terminal running project scripts.

### 7.3 Kafka Topics (first run)
```powershell
docker exec kafka kafka-topics --create --topic network-logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic ddos-alerts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic spark-alerts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 7.4 Typical Live Run (each in its own terminal)
1. `python scripts\log_producer.py --mode normal --duration 5 --ddos-prob 0.7`
2. `python scripts\ddos_detector.py`
3. `python scripts\mahout_ml_detector.py`
4. `python scripts\alert_monitor.py`
5. Optional add-ons:
   - `python scripts\hdfs_storage.py`
   - `python scripts\hbase_anomaly_store.py`
   - `python scripts\dashboard.py`
   - `python scripts\bigdata_analyzer.py --analysis all`

### 7.5 Spark Streaming on Windows
Running `spark_stream_detector.py` locally requires Java 17 and `winutils` in `HADOOP_HOME`. Without those dependencies, the Dockerized Spark master/worker still satisfy the complete-ecosystem requirement, and other services remain functional.

### 7.6 Stopping and Resetting
```powershell
docker compose down
# or clean volumes
docker compose down -v
```
Terminate Python processes with Ctrl+C before shutting down containers.

## 8. Analytics, Reporting, and ML Lifecycle
- **HDFS-backed analytics**: `bigdata_analyzer.py` performs MapReduce-style groupings, detects suspicious IPs, and generates threat intelligence tables from batch data.
- **HBase insight**: `hbase_analytics.py --action report` prints alert trends, top offenders, severity distributions, and recommendations; `--action export` writes structured JSON.
- **Distributed training**: `mahout_distributed_trainer.py --sample-fraction 1.0` ingests HDFS logs, engineers features, trains multiple ML models, and writes artefacts to `/ddos/models`.
- **Model refresh cycle**: `mahout_ml_detector.py` collects streaming samples and periodically retrains its ensemble; `ml_model_trainer.py` supports explicit K-Means training and batch scoring runs.

## 9. Observability and Tooling
- Kafka UI: `http://localhost:8080`
- HDFS NameNode web UI: `http://localhost:9870`
- Spark Master UI: `http://localhost:8081` (worker at 8082)
- HBase Master UI: `http://localhost:16010`
- Streamlit dashboard (if launched): `http://localhost:8501`

Command-line verification examples:
```powershell
# Kafka topic list
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
# HDFS tree
docker exec namenode hdfs dfs -ls -R /ddos
# HBase shell
"list" | docker exec -i hbase hbase shell
```

## 10. Repository Structure Overview
```
Network_Log_Analysis/
├── README.md                 # Quickstart instructions (updated)
├── PROJECT_OVERVIEW.md       # This comprehensive handbook
└── kafka-ddos-detection/
    ├── docker-compose.yml    # Containerized Hadoop ecosystem
    ├── requirements.txt      # Python dependencies
    ├── config/
    │   ├── hadoop.env        # Hadoop/HDFS/YARN environment variables
    │   └── pipeline_config.json
    ├── scripts/              # All Python services, detectors, analytics, orchestration
    ├── models/               # Serialized ML artefacts (local cache)
    ├── spark-checkpoints/    # Structured Streaming checkpoints (local)
    └── docs/ (guides)        # Supplemental documentation files
```
Refer to `HADOOP_ECOSYSTEM_GUIDE.md`, `HADOOP_INTEGRATION_SUMMARY.md`, and `HOW_TO_RUN.md` within `kafka-ddos-detection` for more narrative and tutorial content.

## 11. Suggested Next Steps
- Tailor detection thresholds in `pipeline_config.json` to match your traffic patterns.
- Extend `log_producer.py` to ingest real packet captures or live network data.
- Add CI jobs or notebooks that automate periodic model retraining with `mahout_distributed_trainer.py`.
- Integrate authentication or alert forwarding to an incident response system for production scenarios.
