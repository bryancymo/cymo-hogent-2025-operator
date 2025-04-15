# operator.py
import kopf
import os
import logging
from kubernetes import client, config
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException

# --- Configuration ---
# Fetch Confluent Cloud bootstrap servers from environment variable
# You MUST set this in your deployment manifest.
BOOTSTRAP_SERVERS = 'http://pkc-z1o60.europe-west1.gcp.confluent.cloud:9092/'
# Name of the secret containing Confluent Cloud credentials
SECRET_NAME = 'confluent-credentials'

# --- Logging Setup ---
# Configure Kopf logging level if needed (optional)
# kopf.configure(verbose=True) # or debug=True
logger = logging.getLogger('confluent-operator')
if not logger.hasHandlers():
    logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


# --- Helper Function to get Kafka Admin Client ---
def get_kafka_admin_client(namespace: str):
    """
    Retrieves Confluent credentials from a Kubernetes secret
    and returns an authenticated Kafka AdminClient.
    """
    if not BOOTSTRAP_SERVERS:
        raise kopf.PermanentError("CONFLUENT_BOOTSTRAP_SERVERS environment variable not set.")

    try:
        # Load Kubernetes configuration (use in-cluster config if running in pod)
        try:
            config.load_incluster_config()
        except config.ConfigException:
            logger.warning("Could not load in-cluster config. Trying local kubeconfig.")
            config.load_kube_config() # Fallback for local testing

        # Create Kubernetes API client
        v1 = client.CoreV1Api()

        # Read the secret
        logger.info(f"Fetching secret '{SECRET_NAME}' from namespace '{namespace}'")
        secret = v1.read_namespaced_secret(name=SECRET_NAME, namespace=namespace)

        # Decode the credentials (Secrets are base64 encoded)
        api_key = client.V1Secret().api_client.deserialize(secret, 'V1Secret').data['API_KEY']
        api_secret = client.V1Secret().api_client.deserialize(secret, 'V1Secret').data['API_SECRET']

        # Decode from base64
        import base64
        decoded_api_key = base64.b64decode(api_key).decode('utf-8')
        decoded_api_secret = base64.b64decode(api_secret).decode('utf-8')

        # Kafka Admin Client Configuration
        conf = {
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': decoded_api_key,
            'sasl.password': decoded_api_secret,
        }
        admin_client = AdminClient(conf)
        logger.info("Successfully created Kafka AdminClient.")
        return admin_client

    except client.ApiException as e:
        logger.error(f"Error accessing Kubernetes secret '{SECRET_NAME}' in namespace '{namespace}': {e.status} {e.reason}")
        # If secret is not found, it's a config error we might not recover from
        if e.status == 404:
             raise kopf.PermanentError(f"Secret '{SECRET_NAME}' not found in namespace '{namespace}'.")
        raise kopf.TemporaryError(f"Kubernetes API error fetching secret: {e.reason}. Retrying...", delay=30)
    except Exception as e:
        logger.error(f"Failed to initialize Kafka AdminClient: {e}")
        # Use TemporaryError to retry if it's a transient issue, PermanentError if setup related
        raise kopf.TemporaryError(f"Error setting up Kafka client: {e}. Retrying...", delay=30)


# --- Kopf Handler for ApplicationTopic Creation ---
@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_topic_handler(spec: kopf.Spec, meta: kopf.Meta, namespace: str, logger: kopf.Logger, **kwargs):
    """
    Handles the creation of an ApplicationTopic resource.
    Creates the corresponding topic in Confluent Cloud.
    """
    topic_name = spec.get('name')
    partitions = spec.get('partitions', 1) # Default partitions if not specified
    replication_factor = spec.get('replicationFactor', 1) # Default replication factor for Confluent Cloud
    config_spec = spec.get('config', {}) # Get topic config section
    retention_ms = config_spec.get('retentionMs')
    cleanup_policy = config_spec.get('cleanupPolicy')


    if not topic_name:
        raise kopf.PermanentError("Missing 'spec.name' in ApplicationTopic definition.")

    logger.info(f"Received creation request for ApplicationTopic '{meta.get('name')}' in namespace '{namespace}'")
    logger.info(f"Attempting to create Kafka topic '{topic_name}'")
    logger.info(f"  Partitions: {partitions}")
    logger.info(f"  Replication Factor: {replication_factor}")
    logger.info(f"  Retention (ms): {retention_ms}")
    logger.info(f"  Cleanup Policy: {cleanup_policy}")

    try:
        admin_client = get_kafka_admin_client(namespace)

        # Prepare topic configuration
        topic_config = {}
        if retention_ms is not None:
            topic_config['retention.ms'] = str(retention_ms) # Kafka config values are strings
        if cleanup_policy is not None:
            topic_config['cleanup.policy'] = cleanup_policy

        # Define the new topic
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=partitions,
            replication_factor=replication_factor, # Note: Confluent Cloud usually manages this automatically based on cluster type
            config=topic_config if topic_config else None
        )

        # Create the topic
        # The create_topics function returns futures. We wait for them to complete.
        fs = admin_client.create_topics([new_topic])

        # Wait for the future to complete.
        # The result() call will raise an exception if the topic creation failed.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None on success
                logger.info(f"Successfully created Kafka topic '{topic}'")
            except KafkaException as e:
                # Handle specific Kafka errors
                if e.args[0].code() == KafkaException.TOPIC_ALREADY_EXISTS:
                    logger.warning(f"Kafka topic '{topic}' already exists. Treating as success.")
                    # You might want to check if existing config matches desired config here
                elif e.args[0].code() == KafkaException.INVALID_REPLICATION_FACTOR:
                     logger.warning(f"Invalid replication factor specified ({replication_factor}). Confluent Cloud usually sets this automatically. Ignoring specified value if possible, otherwise creation might fail.")
                     # You might need to adjust logic or remove replication_factor depending on CC behavior
                     raise kopf.PermanentError(f"Failed to create Kafka topic '{topic}': {e}. Invalid replication factor.")
                else:
                    logger.error(f"Failed to create Kafka topic '{topic}': {e}")
                    raise kopf.TemporaryError(f"Kafka error creating topic '{topic}': {e}. Retrying...", delay=60)
            except Exception as e:
                logger.error(f"Unexpected error during Kafka topic '{topic}' creation: {e}")
                raise kopf.TemporaryError(f"Unexpected error creating topic '{topic}': {e}. Retrying...", delay=60)

        # Optionally, update the status of the CRD
        # kopf.patch_status(...)

        return {'status': 'created', 'topicName': topic_name} # Return data to store in the CR status

    except kopf.PermanentError as e:
        logger.error(f"Permanent error for topic '{topic_name}': {e}")
        # No retry for permanent errors. Mark CR as failed maybe?
        raise e # Re-raise to let Kopf handle it
    except kopf.TemporaryError as e:
        logger.warning(f"Temporary error for topic '{topic_name}': {e}")
        raise e # Re-raise to let Kopf retry
    except Exception as e:
        logger.exception(f"Unhandled exception processing topic '{topic_name}': {e}")
        # Treat unexpected errors as temporary to allow retry
        raise kopf.TemporaryError(f"Unhandled exception for topic '{topic_name}': {e}. Retrying...", delay=60)


# --- Optional: Handler for Deletion ---
@kopf.on.delete('jones.com', 'v1', 'applicationtopics', optional=True) # Optional=True means operator won't fail if deletion handler fails
def delete_topic_handler(spec: kopf.Spec, meta: kopf.Meta, namespace: str, logger: kopf.Logger, **kwargs):
    """
    Handles the deletion of an ApplicationTopic resource.
    Deletes the corresponding topic in Confluent Cloud. (Use with caution!)
    """
    topic_name = spec.get('name')

    if not topic_name:
        logger.warning(f"Cannot delete topic: Missing 'spec.name' in deleted ApplicationTopic '{meta.get('name')}'")
        return # Nothing to do

    logger.info(f"Received deletion request for ApplicationTopic '{meta.get('name')}'")
    logger.info(f"Attempting to delete Kafka topic '{topic_name}'")

    # WARNING: Deleting topics is destructive. Ensure this is the desired behavior.
    # Consider adding a finalizer to prevent accidental deletion if the operator is down.

    try:
        admin_client = get_kafka_admin_client(namespace)

        # Delete the topic
        fs = admin_client.delete_topics([topic_name], operation_timeout=30)

        # Wait for the deletion to complete
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"Successfully submitted deletion request for Kafka topic '{topic}'")
                # Note: Deletion might take time in the background in Kafka/Confluent Cloud
            except KafkaException as e:
                # Handle specific Kafka errors
                if e.args[0].code() == KafkaException.UNKNOWN_TOPIC_OR_PART:
                    logger.warning(f"Kafka topic '{topic}' not found during deletion. Assuming already deleted.")
                else:
                    logger.error(f"Failed to delete Kafka topic '{topic}': {e}")
                    # Deletion failures might not be easily retryable. Often requires manual intervention.
                    # Depending on policy, maybe raise Temporary or Permanent error, or just log.
                    kopf.event(body=spec, type="Error", reason="KafkaDeletionFailed", message=f"Failed to delete topic {topic}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during Kafka topic '{topic}' deletion: {e}")
                kopf.event(body=spec, type="Error", reason="KafkaDeletionFailed", message=f"Unexpected error deleting topic {topic}: {e}")


    except Exception as e:
         # Catch exceptions during admin client creation or API calls
        logger.exception(f"Error during deletion process for topic '{topic_name}': {e}")
        kopf.event(body=spec, type="Error", reason="OperatorError", message=f"Error during deletion process: {e}")


# --- Entry Point for Running Kopf (if running directly) ---
# Not needed when run via `kopf run ...` command
#if __name__ == '__main__':
#     # Setup for local testing if needed
#    pass