import kopf

@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def on_create_fn(spec, name, namespace, logger, **kwargs):
    topic_name = spec.get('name', name)
    partitions = spec.get('partitions', 1)
    consumers = spec.get('consumers', [])
    config = spec.get('config', {})

    logger.info(f"[ApplicationTopic] Creating topic '{topic_name}' in namespace '{namespace}'")
    logger.info(f"Partitions: {partitions}")
    logger.info(f"Consumers: {consumers}")
    logger.info(f"Config: {config}")
    logger.info(f"Simulated creation complete for topic '{topic_name}'")
