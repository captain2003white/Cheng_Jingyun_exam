import time
from kafka import KafkaAdminClient, KafkaConsumer

class KafkaInspector:
    def __init__(self, bootstrap_servers="localhost:9092"):
        self.bootstrap_servers = bootstrap_servers

    def verify_connectivity(self):
        try:
            admin = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers, request_timeout_ms=2000)
            admin.list_topics()
            admin.close()
            return True
        except Exception:
            return False

    def get_cluster_metadata(self, topic_name="sensor-events"):
        metadata = {"status": "unreachable", "topic": topic_name, "partitions": 0, "brokers": 0}
        try:
            consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers, request_timeout_ms=2000)
            topics = consumer.topics()
            
            if topic_name in topics:
                partitions = consumer.partitions_for_topic(topic_name)
                metadata["partitions"] = len(partitions) if partitions else 0
                metadata["status"] = "active"
                
            brokers = consumer.cluster_meta_data()
            if brokers and "brokers" in brokers:
                metadata["brokers"] = len(brokers["brokers"])
                
            consumer.close()
            return metadata
        except Exception:
            return metadata