# End-to-End IoT Data Engineering Platform Architecture

## 1. System Architecture Diagram

```text
+---------------------------------------------------------------------------------------+
|                                  INFRASTRUCTURE ZONE                                  |
|                                                                                       |
|   +------------------+      +------------------+      +------------------+            |
|   |   Kafka Broker 1 |      |   Kafka Broker 2 |      |   Kafka Broker 3 |            |
|   | (Leader/Follower)| <--> | (Leader/Follower)| <--> | (Leader/Follower)|            |
|   +------------------+      +------------------+      +------------------+            |
|            ^                                                                          |
+------------|--------------------------------------------------------------------------+
             | (Produce JSON Events via partitioned keys)
+------------|------------+
|    DATA GENERATION      |
|                         |
|   +------------------+  |
|   |   producer.py    |  |
|   |(Python Simulator)|  |
|   +------------------+  |
+-------------------------+
             |
             | (Structured Streaming Consumer via spark-sql-kafka)
+------------v--------------------------------------------------------------------------+
|                                 STREAM PROCESSING ZONE                                |
|                                                                                       |
|   +-------------------------------------------------------------------------------+   |
|   |                               spark_pipeline.py                               |   |
|   |  - Schema Parsing (from_json)                                                 |   |
|   |  - Plausibility Filtering (Data Validation)                                    |   |
|   |  - Rule-based Anomaly Detection (is_anomaly)                                  |   |
|   |  - 2-Minute Event Time Watermarking & 5-Minute Windowed Aggregation          |   |
|   +-------------------------------------------------------------------------------+   |
+------------|-------------------------------|------------------------------|-----------+
             | (Append JSON)                 | (Append Parquet)             | (Append Parquet)
             |                               |                              |
+------------v-------------------------------v------------------------------v-----------+
|                                    THREE-ZONE DATA LAKE                               |
|                                                                                       |
|   +---------------------------+   +---------------------------+   +-------------------+   |
|   |         Raw Zone          |   |       Curated Zone        |   |  Consumption Zone |   |
|   |  /tmp/datalake/raw        |   |  /tmp/datalake/curated    |   | /tmp/datalake/... |   |
|   |  Partition: YYYY/MM/DD/HH |   |  Partition: sensor_type/  |   | Partition:        |   |
|   |                           |   |             YYYY/MM/DD    |   | sensor_type       |   |
|   +---------------------------+   +---------------------------+   +-------------------+   |
+--------------------------------------------^------------------------------------------+
                                             |
                                             | (Batch read for Analytics / Experiments)
                                    +-----------------+
                                    |  analytics.py   |
                                    | (Spark SQL Job) |
                                    +-----------------+
```

## 2.Component Descriptions

**Data Generation (`src/producer.py`)**

A standalone Python simulation script representing the edge gateway layer. It models continuous environmental monitoring by emitting JSON strings to the `sensor-events` Kafka topic. It generates physical metrics (Temperature, Humidity, Pressure), enforces partition ordering by mapping sensor types to Kafka keys, and injects a controlled 10% outlier ratio for verification.

**Message Bus (`docker-compose.yml` / Apache Kafka Cluster)**
A 3-node distributed event streaming cluster deployed via Docker Compose. It serves as the decoupled, fault-tolerant ingestion buffer. The core topic `sensor-events` is architected with 3 partitions and a Replication Factor of 3 ($RF=3$) to withstand unexpected broker crashes without data loss.

**Processing Engine (`src/spark_pipeline.py`)**

A PySpark Structured Streaming application driving the core business logic. It handles distributed stateful transformations including:

- Ingestion of raw messages from Kafka.
- Strict schema enforcement via explicit `StructType`.
- Multi-stage business validation (domain limit filtering and independent status flagging).
- Advanced stream synchronization via a 2-minute event-time watermark and 5-minute tumbling windows.

**Analytics Subsystem (`src/analytics.py`)**

A batch processing module executed on historical data lakes to support deep business intelligence. It performs high-level temporal aggregations and serves as the benchmarking framework for storage layer optimizations (Partition Pruning).