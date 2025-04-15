import kopf
import logging
import os
from confluent_kafka.admin import AdminClient, NewTopic

# Set up logging
logging.basicConfig(level=logging.INFO)

# Load Kafka config from environment variables
KAFKA_CONFIG = {
    "bootstrap.servers": os.environ.get("pkc-z1o60.europe-west1.gcp.confluent.cloud:9092"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.environ.get("Z5EPUNZAK6CKB5EX"),
    "sasl.password": os.environ.get("UFdwQlZ0VFNDQURwMnJNVTFGRHc0K0ZTeitpS05ad1lXNjhCSDZvWFZkZz0="),
}

admin_client = AdminClient(KAFKA_CONFIG)

@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_topic(spec, name, namespace, logger, **kwargs):
    topic_name = spec.get("topicName", name)
    partitions = int(spec.get("partitions", 1))
    replication = int(spec.get("replicationFactor", 3))  # Note: Ignored in Confluent Cloud

    logger.info(f"Creating topic '{topic_name}' with {partitions} partitions...")

    new_topic = NewTopic(topic=topic_name, num_partitions=partitions)

    fs = admin_client.create_topics([new_topic])

    try:
        fs[topic_name].result()  # Wait for result or error
        logger.info(f"✅ Topic '{topic_name}' created successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to create topic '{topic_name}': {e}")
        raise kopf.TemporaryError(f"Kafka topic creation failed: {e}", delay=10)
