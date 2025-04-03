import kopf
import kubernetes
from confluent_kafka.admin import AdminClient, NewTopic

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "bootstrap"

def create_kafka_topic(topic_name, partitions, replication_factor, retention_ms, cleanup_policy):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    topic_config = {
        "retention.ms": str(retention_ms),
        "cleanup.policy": cleanup_policy
    }

    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=partitions,
        replication_factor=replication_factor,
        config=topic_config
    )

    futures = admin_client.create_topics([new_topic])

    for topic, future in futures.items():
        try:
            future.result()  # Ensures the operation completes
            print(f"Topic {topic} created successfully.")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

@kopf.on.create("applicationtopics.jones.com", "v1", "applicationtopics")
def create_topic(spec, meta, **kwargs):
    topic_name = spec.get("name")
    partitions = spec.get("partitions", 1)
    retention_ms = spec.get("config", {}).get("retentionMs", 604800000)  # Default: 7 days
    replication_factor = spec.get("config", {}).get("replicationFactor", 1)
    cleanup_policy = spec.get("config", {}).get("cleanupPolicy", "delete")

    create_kafka_topic(topic_name, partitions, replication_factor, retention_ms, cleanup_policy)

    kopf.adopt(meta)  # Ensures proper resource tracking
    kopf.info(meta, reason="Created", message=f"Kafka topic {topic_name} created successfully.")
