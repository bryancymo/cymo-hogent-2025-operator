import kopf
import logging

logging.basicConfig(level=logging.INFO)

# Handler for ApplicationTopic creation
@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_application_topic(spec, name, namespace, logger, **kwargs):
    topic_name = spec.get('name')
    partitions = spec.get('partitions')
    config = spec.get('config', {})
    consumers = spec.get('consumers', [])

    logger.info(f"Creating topic '{topic_name}' in namespace '{namespace}'")
    logger.info(f"Partitions: {partitions}, Config: {config}, Consumers: {consumers}")

    return {"message": f"Topic '{topic_name}' creation simulated."}

# New: Handler for Servicealt creation
@kopf.on.create('jones.com', 'v1', 'servicealts')
def on_servicealt_create(spec, name, namespace, logger, **kwargs):
    logger.info(f"New Servicealt created: '{name}' in namespace '{namespace}'")
    logger.info(f"ContextLink: {spec.get('contextLink')}, Secret: {spec.get('secretSolution')}")

    return {"message": f"Servicealt '{name}' registered successfully."}
