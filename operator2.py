import kopf
import kubernetes
import logging

# Initialize Kubernetes API client
kubernetes.config.load_incluster_config()  # Use in-cluster configuration
v1 = kubernetes.client.CoreV1Api()

# Watch for new "Context" resources
@kopf.on.create('jones.com', 'v1', 'contexts')
def create_context(spec, name, namespace, **kwargs):
    logging.info(f"New Context Created: {name}")
    owner = spec.get("owner", "Unknown")
    developer_groups = spec.get("developerGroups", [])
    logging.info(f"Owner: {owner}, Developer Groups: {developer_groups}")

# Watch for new "Servicealt" resources
@kopf.on.create('jones.com', 'v1', 'servicealts')
def create_servicealt(spec, name, namespace, **kwargs):
    logging.info(f"New Servicealt Created: {name}")
    context_link = spec.get("contextLink", "Not specified")
    secret_solution = spec.get("secretSolution", "No secret provided")
    logging.info(f"Context Link: {context_link}, Secret: {secret_solution}")

# Watch for new "Domaintopic" resources
@kopf.on.create('jones.com', 'v1', 'domaintopics')
def create_domaintopic(spec, name, namespace, **kwargs):
    logging.info(f"New Domain Topic Created: {name}")
    config = spec.get("config", {})
    partitions = spec.get("partitions", "Not specified")
    logging.info(f"Partitions: {partitions}, Config: {config}")

# Watch for new "ApplicationTopic" resources
@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_application_topic(spec, name, namespace, **kwargs):
    logging.info(f"New Application Topic Created: {name}")
    consumers = spec.get("consumers", [])
    replication_factor = spec.get("config", {}).get("replicationFactor", "Not set")
    logging.info(f"Consumers: {consumers}, Replication Factor: {replication_factor}")

