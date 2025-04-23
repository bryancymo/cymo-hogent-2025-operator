import kopf
import logging
import base64
import requests
import time
import random
import traceback
from kubernetes import client, config

logging.basicConfig(level=logging.INFO)

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    settings.watching.namespaces = ['argocd', 'operator']

# Constants
MAX_RETRIES = 5
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


# ApplicationTopic
@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_application_topic(spec, name, namespace, logger, **kwargs):
    logger.info(f"[ApplicationTopic] Created: '{name}' in namespace '{namespace}'")
    retry = kwargs.get("retry", 0)

    topic_name = spec.get("name", name)
    partitions = spec.get("partitions", 3)
    config = spec.get("config", {})
    consumers = spec.get("consumers", [])

    # Replace these with your actual values from Confluent Cloud
    cluster_id = "lkc-n9z7v3"  # <- YOUR cluster ID
    rest_endpoint = "https://pkc-z1o60.europe-west1.gcp.confluent.cloud:443"  # <- YOUR REST endpoint (no trailing slash)

    def run():
        api_key, api_secret = get_topic_credentials(namespace='argocd')
        create_confluent_topic(topic_name, partitions, config, api_key, api_secret, cluster_id, rest_endpoint)
        logger.info(f"[Confluent] Kafka topic '{topic_name}' created successfully")

    retry_with_backoff(run, retry, logger, error_msg="Failed to create Kafka topic")

    return {"message": f"Topic '{topic_name}' created successfully."}



@kopf.on.update('jones.com', 'v1', 'applicationtopics')
def update_application_topic(spec, name, namespace, logger, **kwargs):
    logger.info(f"[ApplicationTopic] Updated: '{name}' in namespace '{namespace}'")
    logger.info(f"Partitions: {spec.get('partitions')}, Config: {spec.get('config')}, Consumers: {spec.get('consumers')}")
    return {"message": f"Topic '{name}' update simulated."}


@kopf.on.delete('jones.com', 'v1', 'applicationtopics')
def delete_application_topic(spec, name, namespace, logger, **kwargs):
    logger.info(f"[ApplicationTopic] Deleted: '{name}' in namespace '{namespace}'")
    return {"message": f"Topic '{name}' deletion simulated."}


# Domaintopic
@kopf.on.create('jones.com', 'v1', 'domaintopics')
def create_domaintopic(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Domaintopic] Created: '{name}' in namespace '{namespace}'")
    logger.info(f"Partitions: {spec.get('partitions')}, Config: {spec.get('config')}, Consumers: {spec.get('consumers')}")
    return {"message": f"Domaintopic '{name}' creation logged."}


@kopf.on.update('jones.com', 'v1', 'domaintopics')
def update_domaintopic(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Domaintopic] Updated: '{name}' in namespace '{namespace}'")
    logger.info(f"Partitions: {spec.get('partitions')}, Config: {spec.get('config')}, Consumers: {spec.get('consumers')}")
    return {"message": f"Domaintopic '{name}' update logged."}


@kopf.on.delete('jones.com', 'v1', 'domaintopics')
def delete_domaintopic(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Domaintopic] Deleted: '{name}' in namespace '{namespace}'")
    return {"message": f"Domaintopic '{name}' deletion logged."}


# Context
@kopf.on.create('jones.com', 'v1', 'contexts')
def create_context(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Context] Created: '{name}' in namespace '{namespace}'")
    logger.info(f"Owner: {spec.get('owner')}, Developer Groups: {spec.get('developerGroups')}")
    return {"message": f"Context '{name}' creation logged."}


@kopf.on.update('jones.com', 'v1', 'contexts')
def update_context(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Context] Updated: '{name}' in namespace '{namespace}'")
    logger.info(f"Owner: {spec.get('owner')}, Developer Groups: {spec.get('developerGroups')}")
    return {"message": f"Context '{name}' update logged."}


@kopf.on.delete('jones.com', 'v1', 'contexts')
def delete_context(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Context] Deleted: '{name}' in namespace '{namespace}'")
    return {"message": f"Context '{name}' deletion logged."}


# Servicealt
@kopf.on.create('jones.com', 'v1', 'servicealts')
def create_servicealt(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Servicealt] Created: '{name}' in namespace '{namespace}'")
    logger.info(f"ContextLink: {spec.get('contextLink')}, SecretSolution: {spec.get('secretSolution')}")

    retry = kwargs.get("retry", 0)

    def run():
        api_key, api_secret = get_confluent_credentials(namespace='argocd')
        sa_response = create_confluent_service_account(name, "Service account for Servicealt", api_key, api_secret)
        logger.info(f"Service account created: ID={sa_response['id']} Name={sa_response['display_name']}")
        return sa_response

    retry_with_backoff(run, retry, logger, error_msg="Failed to create service account")

    return {"message": f"Servicealt '{name}' creation logged, service account created."}


@kopf.on.update('jones.com', 'v1', 'servicealts')
def update_servicealt(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Servicealt] Updated: '{name}' in namespace '{namespace}'")
    logger.info(f"ContextLink: {spec.get('contextLink')}, SecretSolution: {spec.get('secretSolution')}")
    return {"message": f"Servicealt '{name}' update logged."}


@kopf.on.delete('jones.com', 'v1', 'servicealts')
def delete_servicealt(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Servicealt] Deleted: '{name}' in namespace '{namespace}'")
    return {"message": f"Servicealt '{name}' deletion logged."}


# Confluent
logger = logging.getLogger(__name__)

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
    
def get_topic_credentials(namespace='argocd'):
    try:
        logger.info(f"[Confluent] Loading topic credentials from namespace '{namespace}'")
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        secret = v1.read_namespaced_secret("confluent-application-topic-credentials", namespace)
        api_key = base64.b64decode(secret.data['API_KEY']).decode("utf-8")
        api_secret = base64.b64decode(secret.data['API_SECRET']).decode("utf-8")
        logger.info("[Confluent] Topic credentials loaded successfully")
        return api_key, api_secret
    except client.exceptions.ApiException as e:
        raise RuntimeError(f"[Confluent] Failed to read topic secret from namespace '{namespace}': {e}")
    except KeyError as e:
        raise ValueError(f"[Confluent] Missing key in topic secret: {e}")
    except Exception as e:
        raise RuntimeError(f"[Confluent] Error fetching topic credentials: {e}")





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

def get_confluent_service_account_by_name(name, api_key, api_secret):
    logger.info(f"[Confluent] Searching for service account by name: '{name}'")
    url = "https://api.confluent.cloud/iam/v2/service-accounts"
    try:
        response = requests.get(url, auth=(api_key, api_secret))
        response.raise_for_status()
        accounts = response.json().get("data", [])
        logger.debug(f"[Confluent] Retrieved {len(accounts)} service accounts")

        matched = next((acct for acct in accounts if acct.get("display_name") == name), None)
        if matched:
            logger.info(f"[Confluent] Found matching service account: ID={matched.get('id')} Name={matched.get('display_name')}")
        else:
            logger.warning(f"[Confluent] No matching service account found with name: '{name}'")

        return matched
    except requests.HTTPError as e:
        logger.error(f"[Confluent] HTTP error while retrieving service accounts: {e.response.status_code} - {e.response.text}")
        raise
    except Exception as e:
        logger.exception("[Confluent] Unexpected error while retrieving service accounts")
        raise

def create_confluent_topic(name, partitions, config, api_key, api_secret, cluster_id, rest_endpoint):
    url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics"
    headers = {
        "Content-Type": "application/json"
    }

    topic_config = []
    if "retentionMs" in config:
        topic_config.append({"name": "retention.ms", "value": str(config["retentionMs"])})
    if "cleanupPolicy" in config:
        topic_config.append({"name": "cleanup.policy", "value": config["cleanupPolicy"]})

    payload = {
        "topic_name": name,
        "partitions_count": partitions,
        "replication_factor": config.get("replicationFactor", 3),
        "configs": topic_config
    }

    response = requests.post(url, json=payload, auth=(api_key, api_secret), headers=headers)
    response.raise_for_status()
    return response.json()

