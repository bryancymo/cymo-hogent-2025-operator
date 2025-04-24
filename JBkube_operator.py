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


logging.basicConfig(level=logging.INFO)

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    settings.watching.namespaces = ['argocd', 'operator']

# ApplicationTopic
@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_application_topic(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Applicationtopic] Created: '{name}' in namespace '{namespace}'")
    logger.info(f"Partitions: {spec.get('partitions')}, Config: {spec.get('config')}, Consumers: {spec.get('consumers')}")
    return {"message": f"Applicationtopic '{name}' creation logged."}


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
        # Retrieve Confluent operator API
        mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace='argocd')

        # Create service account
        sa_name = f"operator-hogent-{name}"
        sa_response = create_confluent_service_account(sa_name, f"Service account for {name}", mgmt_api_key, mgmt_api_secret)
        sa_id = sa_response['id']
        logger.info(f"[Confluent] Service account created: ID={sa_id} Name={sa_response['display_name']}")

        # Create API key for the new service account
        api_key_data = create_confluent_api_key(sa_id, mgmt_api_key, mgmt_api_secret)
        new_api_key = api_key_data['key']
        new_api_secret = api_key_data['secret']
        logger.info(f"[Confluent] API Key created for service account '{name}'")

        # Create Kubernetes Secret to store API credentials
        secret_name = f"confluent-{sa_name}-credentials"
        create_k8s_secret(namespace, secret_name, new_api_key, new_api_secret, sa_id)

        return {"service_account_id": sa_id, "secret_name": secret_name}

    result = run()
    return {"message": f"Servicealt '{name}' created with service account and API key stored in secret '{result['secret_name']}'."}


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


