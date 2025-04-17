import kopf
import logging
import base64
import requests
import time
import random
import traceback
from kubernetes import client, config

logging.basicConfig(level=logging.INFO)

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
    logger.info(f"Partitions: {spec.get('partitions')}, Config: {spec.get('config')}, Consumers: {spec.get('consumers')}")
    return {"message": f"Topic '{name}' creation simulated."}


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


# Confluent helpers
def get_confluent_credentials(namespace='argocd'):
    try:
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        secret = v1.read_namespaced_secret("confluent-credentials", namespace)
        api_key = base64.b64decode(secret.data["API_KEY"]).decode("utf-8")
        api_secret = base64.b64decode(secret.data["API_SECRET"]).decode("utf-8")
        return api_key, api_secret
    except client.exceptions.ApiException as e:
        raise RuntimeError(f"Failed to read secret from namespace '{namespace}': {e}")
    except KeyError as e:
        raise ValueError(f"Missing key in secret: {e}")
    except Exception as e:
        raise RuntimeError(f"Error fetching Confluent credentials: {e}")


def create_confluent_service_account(name, description, api_key, api_secret):
    url = "https://api.confluent.cloud/iam/v2/service-accounts"
    payload = {
        "display_name": name,
        "description": description
    }
    response = requests.post(url, json=payload, auth=(api_key, api_secret))
    response.raise_for_status()
    return response.json()
