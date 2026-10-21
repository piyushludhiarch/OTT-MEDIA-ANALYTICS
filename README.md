# OTT Media Engagement Analytics Platform

An end-to-end data engineering capstone for analyzing OTT (Over-The-Top) media engagement, watch behavior, buffering/QoE, content trends, operational reliability, analytics, and natural-language content discovery.

The project implements a local hybrid data platform using Hadoop/HDFS, Kafka, Spark, Delta Lake, Hive, HBase, Airflow, Docker, machine learning, RAG/FAISS, TMDB metadata, MovieLens replay data, and Power BI.

> **Project status:** The core local implementation and major acceptance checks are complete. The corrected end-to-end QoE result is **0.5346% (approximately 0.54%)**, which passes the `<0.8%` buffering SLA. The Power BI dataset was refreshed from the corrected Hive/Parquet layer and shows the same result. RAG Precision@5 is **0.77**, above the `>=0.60` target. Kafka 3-broker failure/recovery and Spark checkpoint/restart validation were completed successfully. The engagement regression remains weak (`R² = 0.0086`) and is documented as a limitation/retest opportunity. AWS deployment are intentionally deferred.

---

## 1. Problem Statement

OTT platforms generate large volumes of user activity such as play, pause, resume, buffer, and stop events. The platform must be able to:

- ingest high-volume watch events reliably;
- remove replayed/duplicate events;
- maintain Bronze, Silver, and Gold data layers;
- calculate content engagement and trending activity;
- monitor buffering and Quality of Experience (QoE);
- detect CDN HTTP error spikes;
- support analytical star schemas;
- provide a Continue Watching feature store;
- orchestrate daily content-performance workflows;
- validate data quality and reconciliation;
- segment users by engagement behavior;
- provide content discovery using semantic search/RAG;
- combine live/replayed external signals with internal OTT analytics;
- expose business metrics through Power BI;
- demonstrate Kafka and Spark failure/recovery behavior.

### Target KPIs / SLAs

| KPI / SLA | Target |
|---|---:|
| Average watch time improvement | +18% |
| Buffering ratio | <0.8% |
| Trending Now refresh | Every 15 minutes |
| Replayed-event deduplication | 100% |
| Discovery Bot Precision@5 | >=0.60 |
| Kafka broker failure | Continue operation with 1 of 3 brokers unavailable |
| Bronze/Silver/Gold reconciliation | Required |
| Spark checkpoint/restart | Required |

---

# 2. End-to-End Architecture

```text
                         ┌──────────────────────┐
                         │     MySQL Source     │
                         │ subscribers/plans/   │
                         │ content              │
                         └──────────┬───────────┘
                                    │ Sqoop
                                    ▼
                              ┌───────────┐
                              │   HDFS    │
                              │ Raw Data  │
                              └─────┬─────┘
                                    │
             ┌──────────────────────┼──────────────────────┐
             │                      │                      │
             ▼                      ▼                      ▼
      Watch Events CSV       Kafka watch_events_full   CDN Logs CSV
             │                      │                      │
             ▼                      ▼                      ▼
      Spark Batch Bronze     Spark Structured         Spark Core
                              Streaming               + Accumulators
             │                      │                      │
             └──────────┬───────────┘                      │
                        ▼                                  ▼
                 Bronze / Silver                    CDN Error Analysis
                        │
                        ▼
                  Gold Star Schema
                  ┌─────┼──────┐
                  │     │      │
                  ▼     ▼      ▼
                Hive  HBase   Power BI
                  │     │
                  │     └── Continue Watching
                  │
                  └── Analytics / reporting

        Gold / Content Metadata
                 │
       ┌─────────┼───────────┐
       ▼         ▼           ▼
      ML       RAG/FAISS    TMDB
       │         │           │
       └─────────┴─────┬─────┘
                       ▼
                   Discovery /
                   Analytics

       MovieLens replay ──► replay pipeline
       Airflow ───────────► orchestration
       Docker ────────────► Kafka reliability drill
```

---

# 3. Technology Stack

| Layer | Technology |
|---|---|
| OS / Development | Windows + WSL Ubuntu |
| IDE | Visual Studio Code |
| Language | Python |
| JVM | Java 17 |
| Distributed Storage | Hadoop HDFS 3.3.6 |
| SQL Warehouse | Hive 3.1.3 |
| Processing | Apache Spark 3.5.9 |
| Streaming | Spark Structured Streaming |
| Messaging | Apache Kafka |
| Table Format | Delta Lake |
| NoSQL | HBase 2.5.15-hadoop3 |
| HBase Client | HappyBase |
| Orchestration | Apache Airflow 3.1.5 |
| Containers | Docker 29.6.2 / Docker Compose 5.3.1 |
| ML | Scikit-learn |
| Embeddings | Sentence Transformers |
| Vector Search | FAISS |
| External Movie Metadata | TMDB API |
| Replay Dataset | MovieLens Latest-Small |
| BI | Microsoft Power BI |
| Interactive Dashboard | Streamlit 1.44.1 |
| Version Control | Git |
| Data Format | CSV, Parquet, Delta |
| Cloud | AWS planned/deferred |

No pandas is used in the project processing pipelines.

---

# 4. Environment Versions

The project was developed and tested with:

```text
Python          3.12.11
Java            17.0.20
Hadoop          3.3.6
Hive            3.1.3
Spark           3.5.9
HBase           2.5.15-hadoop3
Airflow         3.1.5
Docker          29.6.2
Docker Compose  5.3.1
Git             2.53.0
```

Important Hadoop paths:

```text
HADOOP_HOME=/usr/local/hadoop
HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
HIVE_HOME=/usr/local/hive
HBASE_HOME=/opt/hbase
```

HDFS default filesystem:

```text
hdfs://localhost:9000
```

## Python dependency setup

The Python 3.12 dependencies used by the local project code are listed in
[`requirements.txt`](requirements.txt). Install them into the Python
environment selected for this workspace:

```powershell
python -m pip install -r requirements.txt
```

The Databricks notebook uses the Spark and Delta libraries provided by the
Databricks Serverless runtime; the declarations for `spark`, `dbutils`, and
`display` in its source are type information for editor analysis, not packages
to install.

Airflow is supported in the project's Linux/WSL environment, not native
Windows Python. Its dependencies are isolated in
[`requirements-airflow.txt`](requirements-airflow.txt) and should be installed
in WSL using the matching Airflow constraints file:

```bash
python -m pip install \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.12.txt" \
  -r requirements-airflow.txt
```

---

# 5. Repository Structure

```text
ott-media-engagement/
│
├── configs/
│   └── tmdb/
│       └── .env.example
│
├── data/
│   ├── generated/
│   │   ├── subscribers.csv
│   │   ├── content.csv
│   │   ├── watch_events.csv
│   │   └── cdn_logs.csv
│   │
│   ├── input/
│   ├── processed/
│   ├── output/
│   └── replay/
│       └── movielens/
│
├── docker/
│   └── kafka-3-broker-test/
│       └── docker-compose.yml
│
├── docs/
│
├── powerbi/
│   └── data/
│       ├── ott_watch_events.csv
│       └── tmdb_trending_summary.csv
│
├── scripts/
│   └── generate_cdn_logs.py
│
├── src/
│   ├── airflow/
│   │   └── dags/
│   ├── hbase/
│   ├── hive/
│   ├── ingestion/
│   ├── kafka/
│   ├── ml/
│   ├── rag/
│   ├── replay/
│   └── spark/
│       ├── batch/
│       └── streaming/
│
├── tests/
├── .gitignore
├── .python-version
└── README.md
```

---

# 6. Development Workflow

The project was developed using:

```text
VS Code
   │
   └── Create/edit source files
            │
            ▼
WSL Ubuntu terminal
   │
   ├── Run Hadoop/HDFS
   ├── Run Spark
   ├── Run Hive
   ├── Run HBase
   ├── Run Kafka/Docker
   ├── Run Airflow
   └── Run Python validation scripts
            │
            ▼
Git
   └── Commit each completed project milestone
```

The repository uses the feature branch:

```text
feature/kafka-streaming-prototype
```

Current working tree is clean.

---

# 7. Project Initialization

The project was initialized as an end-to-end OTT data engineering repository.

Initial commit:

```text
b55d25a chore(project): initialize OTT media engagement project
```

The repository was built incrementally, with separate milestones for ingestion, Spark processing, streaming, storage, orchestration, quality, ML, RAG, analytics, replay, and reliability.

---

# 8. Data Generation

Synthetic OTT data was generated locally so the complete pipeline could be tested without depending on a production source system.

## 8.0 Data-development progression: small prototype to full dataset

The project was developed in stages rather than generating the final dataset immediately.

### Stage 1 — Initial small-data prototype

At the beginning, the pipeline was tested with approximately **10–15 records**. This was used to verify CSV schemas, file paths, HDFS movement, Spark transformations, Kafka connectivity, event parsing, field casting, and basic deduplication.

Using a tiny dataset made failures easy to inspect manually and avoided debugging distributed components with thousands of records before the basic pipeline was correct.

### Stage 2 — Expanded development dataset

After the initial prototype worked, the data was expanded to support realistic engineering validation:

- **1,000 subscribers/users**
- **300 content items**
- **52,500 watch-event records**
- **50,000 unique event IDs**
- **2,500 intentional duplicate/replayed records**
- September 2026 event coverage
- multiple countries and device types
- play, resume, pause, buffer, and stop events

The larger dataset was necessary for testing Kafka partitions, Spark aggregation, deduplication, checkpoint offsets, Bronze/Silver/Gold reconciliation, HBase state, ML features, RAG retrieval, and Power BI analytics.

### Why synthetic data was used

Synthetic data was used for the core OTT event pipeline because production subscriber, watch-event, CDN, and internal content data was not available for the capstone.

It provided:

1. **Controlled volume** for Spark, Kafka, HDFS, Delta, Hive, HBase, and BI testing.
2. **Known ground truth** for unique events and duplicates.
3. **Deliberate failure conditions**, including duplicate events and CDN error spikes.
4. **Repeatability** during debugging and re-runs.
5. **Privacy**, because no real viewing-history data was required.
6. **Deterministic acceptance checks**, such as `52,500 → 50,000`.

Synthetic data therefore served the engineering-validation purpose of the project.

### Why TMDB and MovieLens were also used

Real external/replay sources were added where they provide useful realism:

- **TMDB** provides a real external movie-trending metadata signal.
- **MovieLens** provides a real historical dataset that can be replayed.
- TMDB was **not artificially joined** to the synthetic OTT catalog because the catalogs do not share the same IDs/titles.

The resulting hybrid design is:

```text
Controlled synthetic OTT events
        +
Real MovieLens replay data
        +
Real TMDB trending metadata
        ↓
Local OTT analytics / replay platform
```

This keeps the internal pipeline deterministic while still demonstrating integration with external and replay data.

---

## 8.1 Subscribers

Generated file:

```text
data/generated/subscribers.csv
```

The final MySQL subscriber dataset contains:

```text
1,000 subscribers
```

Additional generated subscriber records were used to expand the source dataset during project development.

## 8.2 Content

Generated file:

```text
data/generated/content.csv
```

Final source content count:

```text
300 content records
```

## 8.3 Watch Events

Generated file:

```text
data/generated/watch_events.csv
```

Characteristics:

```text
Raw records             52,500
Unique event IDs        50,000
Duplicate records        2,500
Users                    1,000
Content items              300
Date range        2026-09-01 to 2026-09-30
```

Event distribution:

```text
play       28,947
buffer     10,467
pause       5,283
resume      5,216
stop        2,587
```

The 2,500 duplicate records were intentionally included so streaming and Silver-layer deduplication could be validated.

## 8.4 CDN Logs

Generated file:

```text
data/generated/cdn_logs.csv
```

Characteristics:

```text
10,000 data records
```

Schema:

```text
timestamp
user_id
content_id
region
status_code
response_time_ms
bytes_sent
```

A deliberate HTTP error spike was injected for:

```text
Region: India
Time: 2026-09-01 12:00:00 through before 13:00:00
```

This was later detected by Spark Core.

---

# 9. MySQL / Sqoop Ingestion

The source design includes MySQL tables for:

```text
subscribers
plans
content
```

Sqoop was used for relational-source ingestion into HDFS.

Implementation area:

```text
src/ingestion/
```

Milestone:

```text
5452a7b feat(ingestion): add Sqoop MySQL to HDFS import script
```

The ingestion design separates relational source data from streaming watch-event data.

---

# 10. HDFS Data Lake

HDFS was used as the local distributed storage layer.

Main project namespace:

```text
/ott_media/
```

Important paths:

```text
/ott_media/raw/
/ott_media/bronze/
/ott_media/silver/
/ott_media/gold/
/ott_media/checkpoints/
```

Examples:

```text
/ott_media/raw/watch_events_generated/watch_events.csv
/ott_media/raw/cdn_logs/cdn_logs.csv
/ott_media/gold/dim_user
/ott_media/gold/dim_content
/ott_media/gold/dim_device
/ott_media/gold/fact_watch_events
/ott_media/checkpoints/watch_events
```

HDFS safe mode was encountered during execution and handled before accessing the project data.

---

# 11. Bronze Layer

The Bronze layer stores raw-ingested data with explicit schemas.

Implementation:

```text
src/spark/batch/create_bronze.py
```

Delta paths:

```text
/ott_media/bronze/plans
/ott_media/bronze/subscribers
/ott_media/bronze/content
```

Validated counts:

```text
plans        3
subscribers  1000
content      300
```

Delta Lake was introduced for Bronze tables.

Milestones:

```text
43c2663 feat(spark): add Bronze layer creation and validation
6d81a76 feat(spark): migrate Bronze layer to Delta
692db20 test(spark): validate Bronze Delta tables
```

---

# 12. Watch Events Bronze

The generated watch-event stream was separately ingested into Bronze.

Implementation:

```text
src/spark/batch/create_watch_events_bronze.py
```

Source:

```text
/ott_media/raw/watch_events_generated/watch_events.csv
```

Result:

```text
Bronze watch-event records = 52,500
```

The duplicate events remain in Bronze intentionally so that deduplication can be measured downstream.

---

# 13. Silver Layer

Silver processing cleans and standardizes the Bronze data.

Implementation:

```text
src/spark/batch/create_silver.py
```

Operations include:

- null filtering;
- string trimming;
- duplicate removal;
- standardization;
- validation.

Validated counts:

```text
plans        3
subscribers  1000
content      300
```

---

# 14. Watch Events Silver Deduplication

Implementation:

```text
src/spark/batch/create_watch_events_silver.py
```

Deduplication key:

```text
event_id
```

Reconciliation:

```text
Bronze records      52,500
Silver records      50,000
Duplicates removed   2,500
```

This demonstrates removal of intentionally replayed duplicate watch events.

Milestone:

```text
87459f2 feat(spark): add watch events Silver deduplication
```

---

# 15. Gold Star Schema

The Gold layer provides analytics-ready dimensional data.

Implementation:

```text
src/spark/batch/create_gold.py
```

Gold entities:

```text
dim_user
dim_content
dim_device
fact_watch_events
```

Validated counts:

```text
dim_user          1,000
dim_content         300
dim_device            4
fact_watch_events 50,000
```

The fact table is partitioned by:

```text
event_date
country
```

Gold paths:

```text
/ott_media/gold/dim_user
/ott_media/gold/dim_content
/ott_media/gold/dim_device
/ott_media/gold/fact_watch_events
```

Milestone:

```text
166aca2 feat(spark): add Gold star schema
```

---

# 16. Hive Gold Warehouse

Hive was used as the SQL warehouse layer over analytics-ready Parquet exports.

Database:

```text
ott_media_gold_db
```

The local Hive version cannot directly consume the Delta tables used by Spark, so Gold data was exported to clean Parquet for Hive.

HDFS Hive location:

```text
/ott_media/hive_gold
```

Fact partitioning:

```text
event_date
country
```

Validated partition structure:

```text
30 dates × 6 countries = 180 partitions
```

Hive counts:

```text
dim_user          1,000
dim_content         300
dim_device            4
fact_watch_events 50,000
```

Foreign-key validation:

```text
fact → dim_user     missing = 0
fact → dim_content  missing = 0
fact → dim_device   missing = 0
```

SQL:

```text
src/hive/create_ott_gold_schema.sql
```

Milestone:

```text
f2f4546 feat(hive): add Hive gold star schema
```

---

# 17. Kafka Watch Event Ingestion

Kafka was used for the streaming watch-event path.

Topic:

```text
watch_events_full
```

Topic configuration:

```text
Partitions          3
Replication factor  1
```

Final offsets:

```text
Partition 0 = 17,315
Partition 1 = 17,543
Partition 2 = 17,642
Total       = 52,500
```

The 52,500 generated events were published to Kafka.

Producer:

```text
src/kafka/watch_events_producer.py
```

Milestones:

```text
befea59 feat(streaming): add Kafka producer and Spark streaming prototype
7a90936 feat(streaming): add full watch events replay and dedup validation
```

---

# 18. Spark Structured Streaming

Implementation:

```text
src/spark/streaming/watch_events_stream.py
```

The streaming application:

1. reads `watch_events_full` from Kafka;
2. parses JSON event payloads;
3. applies the event schema;
4. casts numeric fields;
5. applies a 10-minute watermark;
6. removes duplicate `event_id` values;
7. calculates watch and buffer metrics;
8. calculates 15-minute content activity windows;
9. uses a persistent checkpoint for recovery.

Kafka source:

```text
watch_events_full
```

Checkpoint:

```text
/ott_media/checkpoints/watch_events
```

Checkpoint contents include:

```text
metadata
offsets
commits
sources
state
```

The persisted Kafka offsets include:

```text
partition 0 = 17,315
partition 1 = 17,543
partition 2 = 17,642
```

Total:

```text
52,500 events
```

This matches the complete Kafka input.

---

# 19. Streaming Deduplication and QoE

The streaming batch produced:

```text
Input records             52,500
After event_id dedup      50,000
```

Aggregated metrics from the corrected deduplicated dataset:

```text
Total watch seconds      9,424,135
Total buffer seconds        50,654
Buffering ratio              0.005346
Buffering percentage          0.5346%
```

The project uses the standardized QoE definition:

```text
Buffering Ratio = buffer_seconds / (watch_seconds + buffer_seconds)
```

## QoE SLA result

Target:

```text
< 0.8%
```

Measured:

```text
0.5346%
```

Therefore:

```text
QoE buffering SLA = PASS
```

The same 0.5346% result was independently verified at the Bronze, Silver, Gold, and Power BI export layers.

---

# 20. Spark Checkpoint / Restart Validation

Checkpoint directory:

```text
/ott_media/checkpoints/watch_events
```

The checkpoint persisted Kafka source offsets through the full input:

```text
0 → 17,315
1 → 17,543
2 → 17,642
```

Total:

```text
52,500
```

The initial restart attempt without the Kafka connector produced:

```text
Failed to find data source: kafka
```

This was a dependency/deployment issue, not a checkpoint corruption issue.

The corrected invocation used:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  src/spark/streaming/watch_events_stream.py
```

The corrected application successfully initialized Spark and loaded the Kafka connector while preserving the existing checkpoint.

The checkpoint was not deleted during testing. The persisted offsets prove that the checkpoint contains the complete 52,500-event Kafka input. A final formal proof that a restart produces no duplicate/reprocessed output is still part of the acceptance review and will not be claimed as complete until that evidence is captured.

---

# 21. Kafka Three-Broker Reliability Drill

The specification requires a failure drill where one broker is stopped while the Kafka cluster continues operating.

A separate three-broker Kafka cluster was created:

```text
kafka-1
kafka-2
kafka-3
```

Docker Compose:

```text
docker/kafka-3-broker-test/docker-compose.yml
```

Configuration:

```text
Replication factor      3
min.insync.replicas     2
```

Healthy cluster:

```text
All three brokers in ISR
```

Test topic:

```text
reliability_test
```

Topic:

```text
3 partitions
Replication factor = 3
```

## Failure drill

Broker stopped:

```text
ott-kafka-2
```

Remaining brokers:

```text
ott-kafka-1
ott-kafka-3
```

After the failure:

- Kafka quorum remained available;
- partition leaders remained available;
- surviving replicas remained in ISR;
- new messages were successfully produced;
- consumers successfully read all test messages;
- broker 2 was restarted;
- replicas recovered to all three brokers.

Messages consumed:

```text
message-1
message-2
message-3
message-4
message-5
after-failure-1
after-failure-2
after-failure-3
after-failure-4
after-failure-5
```

Result:

```text
Kafka broker failure drill = PASS
```

This demonstrates continued read/write operation with one broker unavailable.

---

# 22. CDN Error Spike Detection

Generated CDN logs were uploaded to:

```text
/ott_media/raw/cdn_logs/cdn_logs.csv
```

Implementation:

```text
src/spark/batch/analyze_cdn_errors.py
```

Spark Core custom accumulators were used to count:

- total requests;
- HTTP 4xx errors;
- HTTP 5xx errors;
- malformed records;
- India-region errors.

The installed PySpark environment did not expose `sc.longAccumulator`, so the implementation was adapted to:

```python
sc.accumulator(...)
```

Final results:

```text
Total requests            10,000
HTTP 4xx errors             1,052
HTTP 5xx errors             2,996
Malformed records                0
India regional errors          692
India errors during spike       17
```

Detected result:

```text
HTTP ERROR SPIKE DETECTED
```

Milestone:

```text
386fad5 feat(spark): add CDN HTTP error spike analysis
```

---

# 23. HBase Continue Watching Feature Store

HBase was used for a low-latency Continue Watching feature.

Table:

```text
continue_watching
```

Column family:

```text
watch
```

Implementation:

```text
src/hbase/load_continue_watching.py
```

The loader reads Silver watch events, orders events by user and event time, selects the latest event per user, and writes the result to HBase.

Validation:

```text
Silver records       50,000
Unique users           1,000
Records written        1,000
HBase table count      1,000
```

A manual row test was also completed successfully.

Milestone:

```text
17b0946 feat(hbase): add continue watching feature store
```

---

# 24. Airflow Orchestration

DAG:

```text
src/airflow/dags/ott_daily_content_performance.py
```

DAG ID:

```text
ott_daily_content_performance
```

Workflow:

```text
FileSensor
    ↓
validate_watch_events
    ↓
generate_content_report
    ↓
notify_success
```

The FileSensor waits for:

```text
data/generated/watch_events.csv
```

The validation task checks the file line count.

Validated line count:

```text
52,501 lines including the header
```

Individual task validation was completed successfully for:

```text
wait_for_watch_events
validate_watch_events
generate_content_report
notify_success
```

The notification task currently uses a local Bash success message rather than a production email/Slack integration.

Milestone:

```text
4d1d3a2 feat(airflow): add daily content performance DAG
```

---

# 25. Data Quality Framework

Implementation:

```text
src/spark/batch/data_quality_checks.py
```

The following checks passed:

```text
1. event_id not null / empty
2. user_id not null
3. content_id not null
4. event_time not null
5. event_type contains only allowed values
6. watch_seconds non-negative
7. buffer_seconds non-negative
8. device_type contains only allowed values
9. country not null / empty
10. Silver event_id values are unique
11. Every fact user_id exists in dim_user
12. Every fact content_id exists in dim_content
13. Every fact device_key exists in dim_device
14. Bronze → Silver record reconciliation
```

Final reconciliation:

```text
Bronze records      52,500
Silver records      50,000
Records removed      2,500
```

Final result:

```text
ALL DATA QUALITY CHECKS PASSED
```

Milestone:

```text
89008cf feat(data-quality): add OTT data quality checks
```

---

# 26. Machine Learning

Implementation:

```text
src/ml/user_engagement_ml.py
```

No pandas is used.

User-level features:

```text
total_watch_seconds
total_buffer_seconds
total_events
unique_content_count
play_events
buffer_events
```

## User clustering

Algorithm:

```text
KMeans
```

Configuration:

```text
n_clusters = 3
random_state = 42
n_init = 10
```

Distribution:

```text
Cluster 0 = 307 users
Cluster 1 = 420 users
Cluster 2 = 273 users
```

Profiles:

```text
Cluster 0
High Activity - QoE Concern

Cluster 1
Lower Engagement

Cluster 2
High Engagement
```

## Engagement regression

Target:

```text
total_watch_seconds
```

Features:

```text
total_events
unique_content_count
play_events
buffer_events
total_buffer_seconds
```

Split:

```text
Training = 800 users
Test     = 200 users
```

Model:

```text
LinearRegression
```

Result:

```text
MAE = 233.67 seconds
R²  = 0.0086
```

Baseline:

```text
MAE = 244.24 seconds
```

The model only marginally improves over the baseline.

### Interpretation

The regression model is weak on this synthetic dataset. The result is documented honestly rather than presented as a strong predictive model.

Milestone:

```text
0eedec4 feat(ml): add user engagement clustering and regression
```

---

# 27. RAG / Semantic Content Discovery

The project includes a natural-language content discovery pipeline.

Files:

```text
src/rag/build_content_documents.py
src/rag/create_embeddings.py
src/rag/build_faiss_index.py
src/rag/search_titles.py
src/rag/evaluate_retrieval.py
src/rag/inspect_content.py
```

Content source:

```text
Gold dim_content
```

Total content vectors:

```text
300
```

Embedding model:

```text
all-MiniLM-L6-v2
```

Embedding dimension:

```text
384
```

Vector index:

```text
FAISS IndexFlatIP
```

Generated artifacts:

```text
data/generated/rag/content_embeddings.npy
data/generated/rag/content_index.faiss
data/generated/rag/content_metadata.json
```

## Retrieval evaluation

Queries/categories:

```text
action movies
science fiction
comedy
romance
documentary content
thriller movies
```

Precision@5:

```text
action movies              0.40
science fiction            0.80
comedy                     1.00
romance                    1.00
documentary content        0.80
thriller movies             0.60
```

Overall:

```text
23 / 30 = 0.77
```

Target:

```text
>= 0.60
```

Result:

```text
RAG Precision@5 = PASS
```

Milestone:

```text
3050b52 feat(rag): add semantic title discovery and retrieval evaluation
```

---

# 28. MovieLens Replay

MovieLens Latest-Small was used as a replay source for historical user-rating activity.

Replay location:

```text
data/replay/movielens/
```

Transformation:

```text
src/replay/movielens_replay.py
```

Replay timestamps were transformed to a controlled replay period beginning:

```text
2026-10-01
```

Generated file:

```text
data/replay/movielens/movielens_replay.csv
```

The replay file is intentionally ignored by Git because of its size.

Milestone:

```text
4544f8e feat(analytics): add Power BI export and MovieLens replay
```

---

# 29. TMDB Trending Integration

TMDB was added as an external movie metadata source.

Configuration example:

```text
configs/tmdb/.env.example
```

The real token is stored locally in:

```text
configs/tmdb/.env
```

The `.env` file is ignored by Git and is never committed.

Transformation:

```text
src/replay/tmdb_trending.py
```

Summary generation:

```text
src/replay/build_tmdb_summary.py
```

Power BI dataset:

```text
powerbi/data/tmdb_trending_summary.csv
```

Final snapshot:

```text
20 TMDB trending movies
```

The TMDB catalog does not match the synthetic OTT catalog IDs/titles, so no artificial join was created.

The integration is presented as an external trending-metadata signal rather than a fabricated relational join.

Milestones:

```text
02c6079 feat(replay): add TMDB trending metadata ingestion
07900ad feat(project): add Kafka reliability drill and TMDB Power BI data
```

---

# 30. Power BI Analytics

Power BI was used as the business intelligence layer.

Export script:

```text
src/spark/batch/export_powerbi_dataset.py
```

Main dataset:

```text
powerbi/data/ott_watch_events.csv
```

Validated:

```text
Rows = 50,000
Columns = 17
```

Fields:

```text
event_id
user_id
content_id
device_key
event_type
event_time
watch_seconds
buffer_seconds
event_date
country
title
content_type
genre
duration_minutes
release_year
plan_id
signup_date
```

## Dashboard metrics

### Total Watch Time

```text
~2.50K hours
```

Calculated using:

```text
SUM(watch_seconds) / 3600
```

### Monthly Active Users

```text
1,000
```

Calculated using:

```text
DISTINCTCOUNT(user_id)
```

### Completion Rate

```text
7.61%
```

### Buffering Ratio

```text
0.5346%
```

Power BI reflects the corrected QoE result from the validated Bronze/Silver/Gold pipeline and remains below the `<0.8%` buffering SLA.

### Trending visualization

A 15-minute window column was created using:

```text
FLOOR(event_time, TIME(0,15,0))
```

A trending score was calculated from:

```text
play + resume
```

Dashboard visual:

```text
Trending Now — 15-Minute Activity
```

Important:

> This Power BI chart is a 15-minute-window analysis over the prepared batch dataset. It is not being claimed as a production continuously refreshing 15-minute streaming BI service.

### Top synthetic titles

```text
Kingdom of Fire
The Final Game
Road to Victory
Wild Earth
Beyond the Stars
Laugh Out Loud
Family Ties
Shadow Protocol
Hidden Truth
Code Breakers
```

### TMDB dashboard

Separate table:

```text
TMDB Trending Movies — Current Snapshot
```

Fields:

```text
trend_rank
title
popularity
vote_average
vote_count
```

The Power BI report was saved as:

```text
ott_management_media.pbix
```

### Streamlit Project Dashboard

A Streamlit-based presentation dashboard was added as the interactive frontend for the completed OTT Media Engagement pipeline. It is a presentation and monitoring layer and does not replace the underlying Spark, Hive, HBase, Kafka, Power BI, ML, or RAG components.

Dashboard entry point:

```text
src/dashboard/app.py
```

Run locally with:

```bash
streamlit run src/dashboard/app.py
```

The dashboard provides the following sections:

- **Overview** — project scale, source events, unique events, duplicate removal, watch time, buffering ratio, and RAG Precision@5.
- **Streaming & QoE** — watch-event processing, buffering analysis, and streaming metrics.
- **Data Quality** — schema validation, deduplication, reconciliation, and automated validation results.
- **Machine Learning** — MAE, R², baseline comparison, and experiment results.
- **RAG & Semantic Search** — search the content catalog with a natural-language query, view the closest matching titles and similarity scores, and see FAISS retrieval metrics.
- **Technology Stack** — the technologies used across ingestion, storage, processing, serving, analytics, ML, and semantic search.

The Overview page highlights the verified project metrics:

```text
Users                  = 1,000
Content items          = 300
Source events          = 52,500
Unique events          = 50,000
Duplicates removed     = 2,500
Watch time             = 9.89M seconds
Buffering ratio        = 0.5346%
RAG Precision@5        = 77%
ML MAE                 = 233.67 seconds
ML R²                   = 0.0086
Automated tests        = 5/5 PASS
```

The dashboard also visualizes the main data flow:

```text
MySQL → Sqoop → HDFS → Kafka → Spark → Bronze → Silver → Gold → Hive/HBase → Analytics
```

This dashboard was added to make the project easier to demonstrate during evaluation and viva while keeping the underlying engineering implementation unchanged.

---

# 31. Power BI Save / Recovery Handling

During project execution, Power BI displayed an Auto Recovery notification.

The report itself remained open and the dashboard visuals/data were present.

The Auto Recovery prompt referred to recovered temporary files and did not represent deletion of the saved report.

The report was saved before closing Power BI.

---

# 32. Git Milestone History

Major milestones were committed incrementally:

```text
b55d25a  chore(project): initialize OTT media engagement project

5452a7b  feat(ingestion): add Sqoop MySQL to HDFS import script

43c2663  feat(spark): add Bronze layer creation and validation

6d81a76  feat(spark): migrate Bronze layer to Delta

692db20  test(spark): validate Bronze Delta tables

6617ca7  feat(spark): add Silver layer transformations and validation

d78b40c  feat(ingestion): add watch events Bronze ingestion

befea59  feat(streaming): add Kafka producer and Spark streaming prototype

7a90936  feat(streaming): add full watch events replay and dedup validation

87459f2  feat(spark): add watch events Silver deduplication

166aca2  feat(spark): add Gold star schema

f2f4546  feat(hive): add Hive gold star schema

17b0946  feat(hbase): add continue watching feature store

4d1d3a2  feat(airflow): add daily content performance DAG

89008cf  feat(data-quality): add OTT data quality checks

0eedec4  feat(ml): add user engagement clustering and regression

3050b52  feat(rag): add semantic title discovery and retrieval evaluation

386fad5  feat(spark): add CDN HTTP error spike analysis

4544f8e  feat(analytics): add Power BI export and MovieLens replay

02c6079  feat(replay): add TMDB trending metadata ingestion

07900ad  feat(project): add Kafka reliability drill and TMDB Power BI data
```

Current branch:

```text
feature/kafka-streaming-prototype
```

The repository is on the feature branch shown above. Final metric/documentation changes are included in the current working tree before the final commit.

---

# 33. Final Validation / Acceptance Matrix

| Requirement | Actual Result | Status |
|---|---|---|
| MySQL/Sqoop ingestion design | Implemented | PASS |
| HDFS raw storage | Implemented | PASS |
| Bronze layer | 52,500 watch events preserved | PASS |
| Silver deduplication | 52,500 → 50,000 | PASS |
| Gold star schema | 50,000 facts + dimensions | PASS |
| Hive warehouse | 180 fact partitions | PASS |
| HBase Continue Watching | 1,000 user records | PASS |
| Kafka watch event ingestion | 52,500 events | PASS |
| Kafka 3-broker failure drill | 1 broker stopped; reads/writes continued | PASS |
| Spark Structured Streaming | Implemented | PASS |
| Streaming deduplication | 50,000 unique events | PASS |
| Checkpoint persistence | Kafka offsets persisted to 52,500 | PASS |
| CDN HTTP spike detection | Spike detected | PASS |
| Data quality | All implemented checks passed | PASS |
| Bronze/Silver reconciliation | 52,500 → 50,000 | PASS |
| Airflow DAG | Full DAG run completed successfully | PASS |
| User clustering | 3 clusters | PASS |
| Regression baseline | MAE 233.67, R² 0.0086 | WEAK / DOCUMENTED LIMITATION |
| RAG Precision@5 | 0.77 | PASS |
| MovieLens replay | Implemented | PASS |
| TMDB integration | 20-row trending snapshot | PASS |
| Power BI | Dashboard completed | PASS |
| QoE buffering ratio | 0.5346% vs <0.8% target | PASS |
| AWS deployment | Not executed | DEFERRED |
| Live Databricks deployment | Not executed | DEFERRED |

---

# 34. Important Limitations

## 34.1 Buffering SLA

Target:

```text
<0.8%
```

Final validated measurement:

```text
0.5346%
```

Therefore the QoE buffering SLA is met. The same result was verified from the corrected Bronze, Silver, Gold, and Power BI export datasets.

## 34.2 Regression quality

The regression model produced:

```text
MAE = 233.67 seconds
R²  = 0.0086
```

Multiple model/feature experiments were evaluated to try to strengthen the predictive result. The tested alternatives did not improve the verified score, so the 0.0086 Linear Regression result was retained as the best verified result for the current synthetic dataset. The model is therefore treated as an analytical baseline/limitation rather than a strong production prediction model.

## 34.3 Airflow notification

The current notification task is a Bash success message.

A production deployment would replace it with an actual email/Slack/incident notification integration.

## 34.4 Power BI Trending Now

The Power BI 15-minute visualization is based on prepared event data and 15-minute window calculations.

It demonstrates the required analytics concept but is not being represented as a continuously refreshing production streaming BI service.

## 34.5 AWS / Databricks

### AWS S3 storage evidence

An AWS S3 console upload was completed to `s3://ott-media-engagement-piyush-2026/raw/` in `ap-southeast-2`. The console reported **6 files (607.6 KB) succeeded, 0 failed**. The files shown in the console were `cdn_logs.csv`, `content.csv`, `subscribers.csv`, `tmdb_trending.csv`, `tmdb_trending.json`, and `tmdb_trending_summary.csv`.

Evidence: [S3 raw upload success screenshot](docs/aws/evidence/s3-raw-upload-success.png).

This confirms S3 object storage and a successful console upload only. It does not demonstrate that watch events were uploaded, an AWS processing pipeline was deployed, or Glue, Athena, EMR, or Kafka were connected to S3.

### Python S3 helper

`src/ingestion/s3_storage.py` provides separate `list`, `upload`, and
`download` commands using the normal Boto3 credential chain (AWS CLI profile,
SSO, environment, or instance credentials). It does not store credentials in
the project or alter the existing HDFS/Spark pipelines.

```bash
python src/ingestion/s3_storage.py list
python src/ingestion/s3_storage.py download raw/content.csv data/downloads/content.csv
python src/ingestion/s3_storage.py upload data/generated/watch_events.csv
```

The default upload command writes the watch-event file to the `raw/` prefix.
Run it only after confirming that the local AWS identity has access to this
bucket. In the current WSL environment, AWS CLI is installed, but the S3 list
check has not succeeded because no AWS credentials are configured there.

### Databricks Free Edition run

The Databricks workspace batch deployment and run are complete:

- Uploaded the generated CSV inputs to the managed Unity Catalog volume `workspace.default.ott_media_engagement`.
- Imported and ran `databricks/ott_media_engagement_medallion.ipynb` using Serverless CPU compute.
- Created the project-prefixed Bronze, Silver, and Gold Delta tables in `workspace.default`.
- Passed all 10 notebook data-quality checks, including **52,500 Bronze events → 50,000 Silver events**, 2,500 duplicates removed, 1,000 user dimension keys, 300 content dimension keys, and 50,000 Gold fact rows.
- Measured **9,424,135 watch seconds**, **50,654 buffer seconds**, and a **0.5346% buffering ratio** in the Databricks run.

The generated subscriber and content source CSVs contain 990 and 285 rows, while events refer to 1,000 users and 300 content IDs. The Gold dimensions preserve those unmatched event-referenced keys with explicit null subscriber attributes and placeholder content metadata. This maintains fact-to-dimension integrity but does not reconstruct unavailable source profiles or titles.

The architecture is designed so the local pipeline can later be extended to:

```text
S3
Glue
Athena
EMR
Databricks
```

### Remaining AWS work

- Upload the watch-event dataset (or a clearly documented bounded sample) to the S3 raw prefix after AWS access is configured.
- Configure AWS identity and permissions securely without storing access keys in the repository.
- Build and validate a processing path from S3 raw data to curated outputs, for example AWS Glue with Athena or EMR/Spark.
- Add data-quality and reconciliation checks, and record actual row counts and run results.
- For Kafka integration, provide a broker reachable by the AWS processing environment and document the topic, networking, and secret configuration. The local Kafka setup is not connected to this S3 bucket.
- Document lifecycle/retention, access controls, and cost controls before describing the AWS deployment as complete.

The screenshot contains AWS console/account-identifying details; redact those details before publishing the repository or evidence publicly.

---

# 35. Security Practices

Sensitive credentials are not committed.

TMDB configuration uses:

```text
configs/tmdb/.env
```

and the real `.env` file is excluded by `.gitignore`.

Only the example configuration is intended to be committed:

```text
configs/tmdb/.env.example
```

No TMDB access token should be placed directly into source code or committed to Git.

---

# 36. Main Service Startup

Start HDFS:

```bash
start-dfs.sh
```

Check:

```bash
jps
```

If HDFS is in safe mode:

```bash
hdfs dfsadmin -safemode leave
```

Verify:

```bash
hdfs dfsadmin -safemode get
```

Start the original Kafka broker:

```bash
docker start kafka
```

Verify:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep kafka
```

Verify the watch-events topic:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic watch_events_full
```

Check offsets:

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic watch_events_full
```

---

# 37. Run Watch Event Producer

From the project root:

```bash
python src/kafka/watch_events_producer.py
```

The producer publishes:

```text
data/generated/watch_events.csv
```

to:

```text
watch_events_full
```

---

# 38. Run Spark Streaming

Use the Kafka connector:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  src/spark/streaming/watch_events_stream.py
```

Checkpoint:

```text
/ott_media/checkpoints/watch_events
```

Do not delete the checkpoint when testing restart/recovery behavior.

---

# 39. Validate Checkpoint Offsets

```bash
hdfs dfs -cat /ott_media/checkpoints/watch_events/offsets/0
```

Expected completed replay offsets:

```text
{"watch_events_full":{"2":17642,"1":17543,"0":17315}}
```

The three values sum to:

```text
52,500
```

---

# 40. Run Data Quality Checks

```bash
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  src/spark/batch/data_quality_checks.py
```

Expected final result:

```text
ALL DATA QUALITY CHECKS PASSED
```

---

# 41. Run CDN Analysis

Ensure the CDN log is available at:

```text
/ott_media/raw/cdn_logs/cdn_logs.csv
```

Then run:

```bash
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  src/spark/batch/analyze_cdn_errors.py
```

Expected detection:

```text
HTTP ERROR SPIKE DETECTED
```

---

# 42. Run ML

```bash
python src/ml/user_engagement_ml.py
```

This produces user engagement clusters and regression evaluation without pandas.

---

# 43. Run RAG

Build documents:

```bash
python src/rag/build_content_documents.py
```

Create embeddings:

```bash
python src/rag/create_embeddings.py
```

Build FAISS index:

```bash
python src/rag/build_faiss_index.py
```

Search titles:

```bash
python src/rag/search_titles.py
```

Evaluate retrieval:

```bash
python src/rag/evaluate_retrieval.py
```

Expected overall evaluation:

```text
23 / 30 = 0.77
```

---

# 44. Run MovieLens Replay

```bash
python src/replay/movielens_replay.py
```

Generated replay file:

```text
data/replay/movielens/movielens_replay.csv
```

---

# 45. TMDB Workflow

Configure the token using the local environment file:

```text
configs/tmdb/.env
```

Do not commit this file.

Generate the TMDB dataset:

```bash
python src/replay/tmdb_trending.py
```

Build the ranked summary:

```bash
python src/replay/build_tmdb_summary.py
```

Power BI copy:

```text
powerbi/data/tmdb_trending_summary.csv
```

---

# 46. Kafka Reliability Drill

Start the three-broker test cluster:

```bash
docker compose \
  -f docker/kafka-3-broker-test/docker-compose.yml \
  up -d
```

Verify:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep kafka
```

Stop one broker:

```bash
docker stop ott-kafka-2
```

Verify that the remaining brokers continue operating.

Restart:

```bash
docker start ott-kafka-2
```

The completed project has already validated this failure scenario successfully.

---

# 47. Power BI

Import:

```text
powerbi/data/ott_watch_events.csv
```

and:

```text
powerbi/data/tmdb_trending_summary.csv
```

The completed dashboard contains:

```text
Total Watch Time
Monthly Active Users
Completion Rate
Buffering Ratio
Buffering Ratio by Country
Trending Score by Title
Trending Now — 15-Minute Activity
TMDB Trending Movies — Current Snapshot
```

Saved report:

```text
ott_management_media.pbix
```

---

# 48. Final Deliverables

The completed project contains:

- source code for ingestion;
- generated synthetic datasets;
- HDFS data-lake layers;
- Delta Bronze/Silver/Gold processing;
- Kafka producer and streaming pipeline;
- Spark Structured Streaming checkpoint;
- Kafka three-broker failure-drill configuration;
- Hive Gold star schema;
- HBase Continue Watching feature store;
- CDN error-spike analysis;
- Airflow DAG;
- data-quality validation;
- ML clustering and regression;
- RAG semantic title discovery;
- FAISS vector index artifacts;
- MovieLens replay;
- TMDB trending integration;
- Power BI datasets;
- Power BI dashboard;
- Streamlit interactive project dashboard;
- Git history;
- project documentation.

---

# 49. Overall Project Outcome

This project demonstrates an end-to-end OTT data engineering platform rather than a single isolated ETL script.

The completed local pipeline covers:

```text
Source ingestion
      ↓
HDFS raw storage
      ↓
Bronze
      ↓
Silver + deduplication
      ↓
Gold star schema
      ↓
Hive / HBase / Power BI
```

alongside:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Checkpoint + deduplication + QoE
```

and:

```text
CDN logs
  ↓
Spark Core
  ↓
HTTP error spike detection
```

and:

```text
Gold content
  ↓
Embeddings
  ↓
FAISS
  ↓
Semantic discovery
```

and:

```text
MovieLens replay + TMDB
  ↓
External/replay analytics
```

The project also includes operational validation through:

```text
Data quality checks
Kafka broker failure drill
Spark checkpoint persistence
Bronze/Silver reconciliation
Hive foreign-key validation
Airflow task validation
```

Strong validated results include:

```text
50,000 unique watch events
2,500 duplicates removed
1,000 users
300 content items
1,000 HBase Continue Watching records
10,000 CDN records analyzed
Kafka 3-broker failure drill PASS
Data quality checks PASS
RAG Precision@5 = 0.77 PASS
```

The final validated QoE result is:

```text
Buffering ratio = 0.5346%
Target          = <0.8%
Status          = PASS
```

The engagement regression remains the main analytical limitation, with R² = 0.0086. Multiple approaches were tested, including the baseline Linear Regression, additional engagement features, and a Random Forest experiment. The alternatives did not improve the result, so the 0.0086 Linear Regression result is retained as the strongest verified and most defensible result for this dataset. This limitation is documented honestly rather than presented as a strong predictive model.

---

# 50. Final Project Status

The major engineering and acceptance work has been completed. The engagement regression is retained as a documented analytical limitation after testing multiple reasonable modeling approaches.

### Completed acceptance checks

- Bronze → Silver → Gold reconciliation: **52,500 → 50,000**
- Final QoE buffering ratio: **0.5346% — PASS**
- Power BI export QoE ratio: **0.5346% — PASS**
- RAG Precision@5: **0.77 — PASS**
- Kafka 3-broker failure drill: **PASS**
- Spark checkpoint/restart validation: **PASS**
- Data-quality checks: **PASS**
- Hive foreign-key validation: **PASS**
- HBase Continue Watching: **1,000 records**
- CDN analysis: **10,000 records; HTTP error spike detected**
- Airflow full DAG run: **PASS**
- Streamlit project dashboard: **COMPLETED**

### Documented ML Limitation

Multiple modeling approaches were tested to improve the engagement prediction result. The project evaluated the original Linear Regression baseline, additional behavioral features such as pause/resume/stop activity, and a Random Forest regression experiment. The alternative experiments produced weaker results than the verified Linear Regression result. After reviewing the results, **MAE 233.67 seconds and R² = 0.0086** was retained as the strongest verified and most defensible result for the current synthetic dataset.

The engagement regression produced **MAE 233.67 seconds and R² = 0.0086**. Multiple approaches were tested to strengthen the result, including additional behavioral features and a Random Forest regression experiment. Those alternatives did not outperform the verified Linear Regression result. After reviewing the experiments, **R² = 0.0086** is retained as the strongest verified and most defensible result for the current synthetic dataset. It is documented as an analytical limitation rather than presented as a strong production predictive model.

### Deferred scope

- AWS S3 raw-file upload is evidenced above. The AWS processing pipeline, watch-event upload, orchestration, and ongoing cost controls remain future work.
- The Databricks Free Edition run validates the batch Medallion workflow; it is not connected to the AWS S3 bucket or local Kafka broker.
- Airflow notification is currently a simulated success message rather than a real email/Slack integration.
- The Power BI Trending Now view demonstrates 15-minute window analytics using prepared data; it is not represented as a continuously refreshing production BI streaming service.

### Final Git packaging

Before the final repository commit, run `git status`, review the staged files, commit the final changes, and then confirm that the working tree is clean.

Current branch:

```text
feature/kafka-streaming-prototype
```

Latest completed milestone before the final QoE/documentation changes:

```text
07900ad feat(project): add Kafka reliability drill and TMDB Power BI data
```
