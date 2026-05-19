# Comprehensive Architecture & Reliability Q&A

---

### Q1: Your pipeline crashes during processing, after writing to the raw zone but before writing to the curated zone. What is the impact on the data? Which checkpoint strategy prevents this issue?

* **Data Impact:** There is **zero permanent data loss**, but a risk of **temporary data duplication (At-Least-Once downstream artifact)**. Since messages were already successfully written to the `raw` zone, they are safely persisted on disk. However, because the crash occurred before writing to the `curated` zone, the offsets for that micro-batch were not committed to the Spark write checkpoint directory. Upon pipeline reboot, Spark will re-read the last uncommitted micro-batch from the source, reprocessing and re-writing those exact records to the `raw` zone and successfully committing them to the `curated` zone.

* **Preventative Checkpoint Strategy:**
  1. **Isolated Directory Checkpointing:** Configuring independent `.option("checkpointLocation", "/path/to/checkpoint/zone")` for each stream sink ensures that read-offsets and write-metadata are tracked atomically.
  2. **Idempotent Storage & Deterministic File Naming:** Utilizing deterministic file output patterns ensures that if a re-run occurs, duplicate micro-batch files either overwrite the previous failed batch or can be seamlessly filtered during consumption via Spark's internal transaction log mechanism.

---

### Q2: You scale the producer up to 50,000 messages per second. In your opinion, what would be the first bottlenecks in your current architecture, and how would you fix them?

* **Bottleneck 1: Single-Node PySpark Driver Environment**
  * *Reason:* The current standalone local Spark setup runs inside a single-node thread. At 50k msgs/sec, memory exhaustion (OOM) will occur due to garbage collection lag during data unpacking and stateful window processing.
  * *Fix:* Transition from a local single-node master to a distributed cluster topology (e.g., Spark on Kubernetes or YARN) with 1 dedicated Master and multiple distributed Executors to parallelize the compute load.

* **Bottleneck 2: Local I/O Bottlenecks & Lockings on Standard Filesystem**
  * *Reason:* The storage layer relies on local disk read/write standard storage OS calls. High-concurrency tasks attempting to append flat Parquet files concurrently will experience severe high-wait disk write lockings.
  * *Fix:* Migrate the storage backbone from a local filesystem to an enterprise distributed object storage service (e.g., AWS S3, Azure ADLS Gen2, or MinIO), and implement **Delta Lake** transaction protocols to enable high-concurrency non-blocking writes.

* **Bottleneck 3: Kafka Broker Ingestion Bottleneck (Partition Limit)**
  * *Reason:* The `sensor-events` topic only contains 3 partitions. A single partition cannot handle high-throughput sequential writing efficiently without hitting disk buffering limits.
  * *Fix:* Scale out the Kafka cluster by increasing the topic partition count to 10+ partitions, ensuring data maps uniformly across multiple distinct drives.

---

### Q3: Compare the advantages and drawbacks of using Kafka as the source of truth for historical data, versus a Parquet data lake. In which scenarios should each be preferred?

| Attribute | Kafka as Historical Source of Truth | Parquet Data Lake (Storage) |
| :--- | :--- | :--- |
| **Advantages** | - Immutable append-only log ensures unified event lineage.<br>- Enables universal data replayability from offset zero for system recoveries. | - Columnar encoding enables superior data compression ratios ($>70\%$).<br>- Supports **Partition Pruning** and predicate pushdown to minimize analytical I/O. |
| **Drawbacks** | - Linear scans required for historical searches; no random access index.<br>- Massive storage costs when retention policies are prolonged. | - Lacks real-time publishing capabilities; inherently file-bound.<br>- Schema evolution requires manual orchestration over historical partitions. |
| **Preferred Scenario** | **Event Sourcing & Microservices Integration:** Best when independent services need to continuously replay historical streams to rebuild state caches. | **Enterprise BI & Machine Learning:** Best for batch heavy workloads, dimensional ad-hoc queries, and deep historical trend analytics. |

---

### Q4: A sensor breaks and emits aberrant values for 2 hours. How does your architecture detect this case? How would you isolate these data points without deleting them?

* **Detection Mechanism:**
  The real-time streaming layer (`src/spark_pipeline.py`) continuously processes inbound traffic against explicit domain boundaries. When values fall outside reasonable physical boundaries (e.g., $T > 100^\circ\text{C}$), the pipeline flags the record by setting `is_anomaly = true`. 
  Simultaneously, the batch layer (`src/analytics.py`) calculates moving aggregate deviations. If a sensor's anomaly count increases significantly within a 2-hour window, alert triggers flag the sensor ID for quarantine.

* **Isolation Without Deletion Strategy:**
  1. **Dynamic Dead-Letter-Queue (DLQ) Flag Routing:** Instead of discarding broken logs, the engine stores them in the standard `curated` parquet layout with `is_anomaly` explicitly set to `true`.
  2. **Storage Partition Isolation:** During the write process, the pipeline can append records into a quarantined path inside the lake:
     `/tmp/datalake/quarantined/sensor_type=co2/year=2026/month=05/...`
  3. **Read Filtering:** Downstream serving APIs and BI dashboards isolate these corrupted artifacts by applying default application filters (`WHERE is_anomaly = false`), preserving the raw anomalous data on disk for subsequent diagnostic auditing.

---

### Q5: You must add a new sensor type co2. Which parts of your pipeline must be modified? Give a precise list of files and changes.

To integrate a new `co2` sensor type into the existing decoupled architecture, modifications are isolated to the configuration and validation layers:

1. **`src/producer.py` (Data Generation Layer)**
   * *Change:* Add `"co2"` to the master array of simulated sensor types. Define its physical normal operating range (e.g., 400 to 2000 ppm) and specify its units (e.g., `"ppm"`) within the random generation loop to allow regular anomaly injection.

2. **`src/spark_pipeline.py` (Stream Processing Layer)**
   * *Change:* Update the plausibility rule validation block inside the transformation stage. Add a conditional check to validate domain limits for `co2` (e.g., flagging events as anomalies if values exceed 5000 ppm or drop below 0 ppm).

3. **`src/api/lake_utils.py` (Serving Layer Abstraction)**
   * *Change:* No structural code changes are required. Because the system utilizes directory partition pruning (`sensor_type=co2`), the dynamic `glob.glob` path crawler will automatically discover and read the new `co2` Parquet partitions as soon as they are flushed by Spark.