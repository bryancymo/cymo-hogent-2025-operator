import kopf
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_topic(spec, name, namespace, logger, **kwargs):
    # Extract necessary fields from the spec
    topic_name = spec['name']
    config = spec.get('config', {})
    consumers = spec.get('consumers', [])
    partitions = spec.get('partitions', 1)
    replication_factor = config.get('replicationFactor', 1)
    retention_ms = config.get('retentionMs', 16800000)  # Default to 7 days in ms
    cleanup_policy = config.get('cleanupPolicy', 'delete')  # Default to 'delete'

    # Log information about the topic to be created
    logger.info(f"Creating topic '{topic_name}' in namespace '{namespace}'")
    logger.info(f"Partitions: {partitions}, Replication Factor: {replication_factor}, Retention: {retention_ms}, Cleanup Policy: {cleanup_policy}")
