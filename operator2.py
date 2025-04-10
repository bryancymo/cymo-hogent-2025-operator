import kopf
import logging

@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_topic(spec, name, namespace, logger, **kwargs):
    logger.info(f"Detected creation of ApplicationTopic: {name}")
    
    topic_name = spec.get('name', name)
    partitions = spec.get('partitions', 1)
    consumers = spec.get('consumers', [])
    config = spec.get('config', {})

    # Simulate the creation of a Kafka topic here
    logger.info(f"Creating Kafka topic '{topic_name}' in namespace {namespace}")
    logger.info(f"Partitions: {partitions}, Consumers: {consumers}")
    logger.info(f"Config: {config}")
    
    # Simulate Kafka topic creation (e.g., connect to Kafka, create topic)
    # Kafka interaction code would go here in a real-world scenario.

    logger.info(f"✅ Topic '{topic_name}' created successfully!")

