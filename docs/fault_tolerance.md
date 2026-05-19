# Fault Tolerance Test

## Before stopping broker

The topic description shows that all three brokers are active and each partition has a leader distributed across brokers.

```bash
(kafka_env) (base) PS D:\za\1\dataengineering\exam> docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
Topic: sensor-events    TopicId: R2GosjVRT_WBr2YvgICSwQ PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: sensor-events    Partition: 0    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2
        Topic: sensor-events    Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
        Topic: sensor-events    Partition: 2    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1
```
## Action

One broker (kafka1) was stopped to simulate a failure:

```bash
(kafka_env) (base) PS D:\za\1\dataengineering\exam> docker stop kafka1
kafka1
```
## After stopping broker

```bash
(kafka_env) (base) PS D:\za\1\dataengineering\exam> docker exec kafka2 kafka-topics --bootstrap-server kafka2:29092 --describe --topic sensor-events
Topic: sensor-events    TopicId: R2GosjVRT_WBr2YvgICSwQ PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: sensor-events    Partition: 0    Leader: 3       Replicas: 3,1,2 Isr: 3,2
        Topic: sensor-events    Partition: 1    Leader: 2       Replicas: 1,2,3 Isr: 2,3
        Topic: sensor-events    Partition: 2    Leader: 2       Replicas: 2,3,1 Isr: 2,3
```

- After stopping broker kafka1, the leadership of some partitions changed to the remaining brokers (kafka2 and kafka3). This shows that Kafka is able to automatically reassign leaders when a broker fails.

- We can also see that kafka1 is no longer present in the ISR list. Only the active brokers remain in-sync, which indicates that Kafka updates its replica state dynamically.

- Even after the failure, each partition still has a leader, so the system continues to operate without interruption.

- This experiment highlights the importance of using a replication factor of 3 and setting min.insync.replicas to 2, which together provide both fault tolerance and data consistency.