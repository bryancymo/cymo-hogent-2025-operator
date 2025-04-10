import kopf
import kubernetes
import logging

# Initialize Kubernetes API client
kubernetes.config.load_incluster_config()  # Use in-cluster configuration
v1 = kubernetes.client.CoreV1Api()

# Watch for new "ApplicationTopic" resources
@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_application_topic(spec, name, namespace, **kwargs):
    logging.info(f"New Application Topic Created: {name}")
    
    # Extract the relevant information from the spec
    consumers = spec.get("consumers", [])
    replication_factor = spec.get("config", {}).get("replicationFactor", "Not set")
    retention_ms = spec.get("config", {}).get("retentionMs", "Not set")
    partitions = spec.get("partitions", "Not specified")
    
    # Log the details of the ApplicationTopic
    logging.info(f"Consumers: {consumers}")
    logging.info(f"Replication Factor: {replication_factor}")
    logging.info(f"Retention (ms): {retention_ms}")
    logging.info(f"Partitions: {partitions}")

    # You can add any future logic here to create related resources or perform other actions.