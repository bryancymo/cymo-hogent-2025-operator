import kopf

@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_topic(spec, name, namespace, logger, **kwargs):
    topic_name = spec.get('name', name)
    partitions = spec.get('partitions', 1)
    consumers = spec.get('consumers', [])
    config = spec.get('config', {})

    logger.info(f"📦 New ApplicationTopic created: {topic_name}")
    logger.info(f"Namespace: {namespace}")
    logger.info(f"Partitions: {partitions}")
    logger.info(f"Consumers: {consumers}")
    logger.info(f"Config: {config}")

    # Simulate topic creation (e.g., to Kafka later)
    logger.info(f"✅ Simulated creation of topic '{topic_name}'")
