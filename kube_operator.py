import kopf
import logging
import base64
import requests
import time
import random
import traceback
import json
from kubernetes import client, config
from kubernetes.client.rest import ApiException


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    settings.watching.namespaces = ['argocd', 'operator']
    settings.watching.cluster_wide = False  # Explicitly set to False since we're watching specific namespaces

# Constants
MAX_RETRIES = 1
MAX_DELAY = 300  # in seconds


# Retry logic
def exponential_backoff(retry: int, base: int = 5, cap: int = MAX_DELAY) -> float:
    delay = min(cap, base * (2 ** retry))
    jitter = random.uniform(0, 5)
    return delay + jitter


def retry_with_backoff(func, retry: int, logger, error_msg="Temporary failure", base_delay=5):
    try:
        return func()
    except Exception as e:
        if retry >= MAX_RETRIES:
            logger.error(f"[Retries] Max retries reached. Error: {str(e)}")
            logger.debug(traceback.format_exc())
            raise

        delay = exponential_backoff(retry, base=base_delay)
        logger.warning(f"[Retries] {error_msg} | Attempt {retry+1}/{MAX_RETRIES} | Retrying in {delay:.1f}s | Error: {str(e)}")
        logger.debug(traceback.format_exc())
        raise kopf.TemporaryError(f"{error_msg}. Retrying...", delay=delay)


# ApplicationTopic - Create
def check_application_topic(topic_name, api_key, api_secret, cluster_id, rest_endpoint, logger):
    url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics/{topic_name}"
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.get(url, auth=(api_key, api_secret), headers=headers)
        if response.status_code == 200:
            logger.info(f"[Confluent] Topic '{topic_name}' already exists")
            return True
        elif response.status_code == 404:
            logger.info(f"[Confluent] Topic '{topic_name}' does not exist")
            return False
        else:
            logger.error(f"[Confluent] Unexpected response checking topic: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"[Confluent] Error checking topic existence: {e}")
        return False

@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_applicationtopic(spec, name, namespace, **kwargs):
    logger.info(f"[ApplicationTopic] Created: '{name}' in namespace '{namespace}'")
    retry = kwargs.get("retry", 0)

    topic_name = spec.get("name", name)
    partitions = spec.get("partitions", 3)
    config = spec.get("config", {})

    # Replace these with your actual values from Confluent Cloud
    cluster_id = "lkc-n9z7v3"  # <- YOUR cluster ID
    rest_endpoint = "https://pkc-z1o60.europe-west1.gcp.confluent.cloud:443"  # <- YOUR REST endpoint

    def run():
        try:
            api_key, api_secret = get_topic_credentials(logger, namespace='argocd')
            logger.info(f"[Confluent] Retrieved credentials for topic '{topic_name}'")
            
            # Check if topic already exists
            if check_application_topic(topic_name, api_key, api_secret, cluster_id, rest_endpoint, logger):
                logger.info(f"[Confluent] Topic '{topic_name}' already exists, skipping creation")
                return
            
            create_confluent_topic(topic_name, partitions, config, api_key, api_secret, cluster_id, rest_endpoint, logger)
            logger.info(f"[Confluent] Kafka topic '{topic_name}' created successfully")
        except Exception as e:
            logger.error(f"[Confluent] Error creating topic '{topic_name}': {e}")
            raise

    retry_with_backoff(run, retry, logger, error_msg="Failed to create Kafka topic")

    return {"message": f"Topic '{topic_name}' creation in progress."}

@kopf.on.update('jones.com', 'v1', 'applicationtopics')
def update_application_topic(spec, status, **kwargs):
    logger.debug(f"Received update event for {spec['name']}")
    logger.debug(f"Spec: {spec}")
    logger.debug(f"Status: {status}")
    logger.debug(f"Event Type: {kwargs.get('type', 'None')}")

    # Proceed with your logic here, if any changes are detected
    if spec.get('replicationFactor') != status.get('replicationFactor', 0):
        status['status'] = {'update_application_topic': {'message': "Topic update simulated."}}
        logger.debug(f"Updated status for {spec['name']}: {status}")
        return {'status': status}
    else:
        logger.debug(f"No update needed for {spec['name']}")
        return {}

@kopf.on.delete('jones.com', 'v1', 'applicationtopics')
def delete_application_topic(spec, name, namespace, **kwargs):
    logger.info(f"[ApplicationTopic] Deleted: '{name}' in namespace '{namespace}'")
    return {"message": f"Topic '{name}' deletion simulated."}

#Applicationtopic - Event
@kopf.on.event('jones.com', 'v1', 'applicationtopics')
def debug_event(event, **kwargs):
    logger.info(f"EVENT: {event['type']} for {event['object']['metadata']['name']}")


# Domaintopic
@kopf.on.create('jones.com', 'v1', 'domaintopics')
def create_domaintopic(spec, name, namespace, status, **kwargs):
    logger.info(f"[Domaintopic] Created: '{name}' in namespace '{namespace}'")
    retry = kwargs.get("retry", 0)

    topic_name = spec.get("name", name)
    partitions = spec.get("partitions", 3)
    config = spec.get("config", {})

    # Replace these with your actual values from Confluent Cloud
    cluster_id = "lkc-n9z7v3"  # <- YOUR cluster ID
    rest_endpoint = "https://pkc-z1o60.europe-west1.gcp.confluent.cloud:443"  # <- YOUR REST endpoint

    def run():
        try:
            api_key, api_secret = get_topic_credentials(logger, namespace='argocd')
            logger.info(f"[Confluent] Retrieved credentials for topic '{topic_name}'")
            
            # Check if topic already exists
            if check_application_topic(topic_name, api_key, api_secret, cluster_id, rest_endpoint, logger):
                logger.info(f"[Confluent] Topic '{topic_name}' already exists, skipping creation")
                return {'status': {'create_domaintopic': {'message': f"Topic '{topic_name}' already exists."}}}
            
            create_confluent_topic(topic_name, partitions, config, api_key, api_secret, cluster_id, rest_endpoint, logger)
            logger.info(f"[Confluent] Kafka topic '{topic_name}' created successfully")
            return {'status': {'create_domaintopic': {'message': f"Topic '{topic_name}' created successfully."}}}
        except Exception as e:
            logger.error(f"[Confluent] Error creating topic '{topic_name}': {e}")
            raise

    try:
        result = retry_with_backoff(run, retry, logger, error_msg="Failed to create Kafka topic")
        return result
    except Exception as e:
        return {'status': {'create_domaintopic': {'message': f"Failed to create topic: {str(e)}"}}}


@kopf.on.update('jones.com', 'v1', 'domaintopics')
def update_domaintopic(spec, status, **kwargs):
    logger.debug(f"Received update event for {spec['name']}")
    logger.debug(f"Spec: {spec}")
    logger.debug(f"Status: {status}")
    logger.debug(f"Event Type: {kwargs.get('type', 'None')}")

    # Proceed with your logic here, if any changes are detected
    if spec.get('replicationFactor') != status.get('replicationFactor', 0):
        status['status'] = {'update_domaintopic': {'message': "Topic update simulated."}}
        logger.debug(f"Updated status for {spec['name']}: {status}")
        return {'status': status}
    else:
        logger.debug(f"No update needed for {spec['name']}")
        return {}


@kopf.on.delete('jones.com', 'v1', 'domaintopics')
def delete_domaintopic(spec, name, namespace, **kwargs):
    logger.info(f"[Domaintopic] Deleted: '{name}' in namespace '{namespace}'")
    return {"message": f"Topic '{name}' deletion simulated."}

# Confluent
def get_confluent_credentials(namespace='argocd'): #RIDVAN NIKS VERANDERENN!!!!!!!!!!!!!!!!!!!!!!!
    try:
        logger.info(f"[Confluent] Loading credentials from namespace '{namespace}'")
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        secret = v1.read_namespaced_secret("confluent-credentials", namespace)
        api_key = base64.b64decode(secret.data['API_KEY']).decode("utf-8")
        api_secret = base64.b64decode(secret.data['API_SECRET']).decode("utf-8")
        logger.info("[Confluent] Credentials loaded successfully")
        return api_key, api_secret
    except client.exceptions.ApiException as e:
        raise RuntimeError(f"[Confluent] Failed to read secret from namespace '{namespace}': {e}")
    except KeyError as e:
        raise ValueError(f"[Confluent] Missing key in secret: {e}")
    except Exception as e:
        raise RuntimeError(f"[Confluent] Error fetching Confluent credentials: {e}")
    
def get_topic_credentials(logger, namespace='argocd'):
    try:
        logger.info(f"[Confluent] Loading credentials from namespace '{namespace}'")
        config.load_incluster_config()
        with client.ApiClient() as api:
            v1 = client.CoreV1Api(api)
            secret = v1.read_namespaced_secret("confluent-application-topic-credentials", namespace)

        api_key = base64.b64decode(secret.data['API_KEY']).decode("utf-8")
        api_secret = base64.b64decode(secret.data['API_SECRET']).decode("utf-8")
        logger.info("[Confluent] Credentials loaded successfully")

        return api_key, api_secret
    except client.exceptions.ApiException as e:
        logger.error(f"[Confluent] Failed to read secret: {e}")
        raise RuntimeError(f"[Confluent] Failed to read secret: {e}")

def create_k8s_secret(namespace, secret_name, api_key, api_secret, service_account_id):
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    secret_data = {
        "API_KEY": base64.b64encode(api_key.encode("utf-8")).decode("utf-8"),
        "API_SECRET": base64.b64encode(api_secret.encode("utf-8")).decode("utf-8"),
        "SERVICE_ACCOUNT_ID": base64.b64encode(service_account_id.encode("utf-8")).decode("utf-8")
    }

    secret_manifest = client.V1Secret(
        metadata=client.V1ObjectMeta(name=secret_name, namespace=namespace),
        data=secret_data,
        type="Opaque"
    )

    try:
        v1.create_namespaced_secret(namespace=namespace, body=secret_manifest)
        logger.info(f"[K8S] Secret '{secret_name}' created in namespace '{namespace}'")
    except ApiException as e:
        if e.status == 409:
            v1.replace_namespaced_secret(name=secret_name, namespace=namespace, body=secret_manifest)
            logger.info(f"[K8S] Secret '{secret_name}' updated in namespace '{namespace}'")
        else:
            logger.error(f"[K8S] Failed to create/update secret '{secret_name}': {e}")
            raise

def create_confluent_api_key(service_account_id, api_key, api_secret):
    url = "https://api.confluent.cloud/iam/v2/api-keys"
    payload = {
        "resource": {"id": "all"},  # or specify a resource ID if needed
        "owner": {"id": service_account_id}
    }

    logger.info(f"[Confluent] Creating API key for service account ID: {service_account_id}")
    response = requests.post(url, json=payload, auth=(api_key, api_secret))

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        logger.error(f"[Confluent] Failed to create API key: {e.response.text}")
        raise

    data = response.json()
    logger.info(f"[Confluent] API key created: {data.get('key')}")
    return data  # Contains both key and secret

def get_all_confluent_credentials(namespace='argocd'):
    try:
        logger.info(f"[Confluent] Loading credentials from namespace '{namespace}'")
        config.load_incluster_config()
        v1 = client.CoreV1Api()

        # List all secrets in the given namespace
        secrets = v1.list_namespaced_secret(namespace)
        
        # Initialize a dictionary to store credentials
        credentials = {}

        for secret in secrets.items:
            # Check if the secret name contains 'confluent' (or any other criteria for filtering)
            if 'confluent' in secret.metadata.name:
                try:
                    api_key = base64.b64decode(secret.data['API_KEY']).decode("utf-8")
                    api_secret = base64.b64decode(secret.data['API_SECRET']).decode("utf-8")
                    # Store the credentials in the dictionary
                    credentials[secret.metadata.name] = {
                        'api_key': api_key,
                        'api_secret': api_secret
                    }
                except KeyError:
                    logger.warning(f"[Confluent] Missing API_KEY or API_SECRET in secret '{secret.metadata.name}'")

        if not credentials:
            logger.warning(f"[Confluent] No relevant secrets found in namespace '{namespace}'")
        else:
            logger.info(f"[Confluent] Found {len(credentials)} relevant secrets.")

        return credentials
    except client.exceptions.ApiException as e:
        raise RuntimeError(f"[Confluent] Failed to read secrets from namespace '{namespace}': {e}")
    except Exception as e:
        raise RuntimeError(f"[Confluent] Error fetching Confluent credentials: {e}")

def create_confluent_service_account(name, description, api_key, api_secret):
    url = "https://api.confluent.cloud/iam/v2/service-accounts"
    payload = {
        "display_name": name,
        "description": description
    }
    
    logger.info(f"[Confluent] Creating service account: '{name}'")
    response = requests.post(url, json=payload, auth=(api_key, api_secret))

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        logger.error(f"[Confluent] Failed to create service account '{name}': {e.response.text}")
        raise

    data = response.json()
    logger.info(f"[Confluent] Service account created: ID={data.get('id')} Name={data.get('display_name')}")
    return data

def create_confluent_topic(name, partitions, config, api_key, api_secret, cluster_id, rest_endpoint, logger):
    url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics"
    headers = {
        "Content-Type": "application/json"
    }

    # Prepare topic configuration
    topic_config = []
    if "retentionMs" in config:
        topic_config.append({"name": "retention.ms", "value": str(config["retentionMs"])})
    if "cleanupPolicy" in config:
        topic_config.append({"name": "cleanup.policy", "value": config["cleanupPolicy"]})

    # Construct the payload for the topic creation request
    payload = {
        "topic_name": name,
        "partitions_count": partitions,
        "replication_factor": config.get("replicationFactor", 3),
        "configs": topic_config
    }

    # Log the payload for debugging
    logger.debug(f"[Confluent] Creating topic with payload: {json.dumps(payload)}")

    response = None  # <-- This line prevents NameError if the request fails early
    try:
        # Make the POST request to create the topic
        response = requests.post(url, json=payload, auth=(api_key, api_secret), headers=headers)

        # Log the response status for debugging
        logger.debug(f"[Confluent] Response Status: {response.status_code}")
        logger.debug(f"[Confluent] Response Body: {response.text}")

        # Raise an exception for unsuccessful response status
        response.raise_for_status()

        # Return the response JSON if the request was successful
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"[Confluent] Failed to create topic: {e}")
        if response is not None:
            logger.error(f"[Confluent] Error response: {response.text}")
        raise  # Reraise the exception so retry logic can handle it
