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

    # Retry Logic
    if retry > 0:
        delay = 60 
        logger.warning(f"[Servicealt] Retry #{retry} - delaying for {delay} seconds to prevent spam.")
        time.sleep(delay)

    try:
        mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace='argocd')
        sa_name = f"operator-hogent-{name}"

        # Check if already exists
        existing_sa = get_confluent_service_account_by_name(sa_name, mgmt_api_key, mgmt_api_secret)
        if existing_sa:
            sa_id = existing_sa['id']
            logger.info(f"[Confluent] Service account already exists: ID={sa_id}")
        else:
            # If not create
            sa_response = create_confluent_service_account(sa_name, f"Service account for {name}", mgmt_api_key, mgmt_api_secret)
            sa_id = sa_response['id']
            logger.info(f"[Confluent] Service account created: ID={sa_id}")

        # Check if already existing key
        existing_keys = get_confluent_api_keys_for_service_account(sa_id, mgmt_api_key, mgmt_api_secret)
        logger.info(f"[Confluent] Checking api key")
        if existing_keys:
            api_key_data = existing_keys[0]
            logger.info(f"[Confluent] Existing API key found for service account: {api_key_data['id']}")
            new_api_key = api_key_data['key']
            new_api_secret = "(existing, not retrievable)"
        else:
            # If not create one
            api_key_data = create_confluent_api_key(sa_id, mgmt_api_key, mgmt_api_secret)
            new_api_key = api_key_data['key']
            new_api_secret = api_key_data['secret']
            logger.info(f"[Confluent] New API key created for service account.")

        # Store in secret
        secret_name = f"confluent-{sa_name}-credentials"
        create_k8s_secret(namespace, secret_name, new_api_key, new_api_secret, sa_id)

        return {"message": f"Servicealt '{name}' processed, credentials stored in secret '{secret_name}'."}

    except Exception as e:
        logger.error(f"[Servicealt] Error occurred: {str(e)}\n{traceback.format_exc()}")
        raise kopf.TemporaryError(f"[Servicealt] Temporary error: {e}", delay=60)



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

def get_confluent_api_keys_for_service_account(service_account_id, api_key, api_secret):
    url = "https://api.confluent.cloud/iam/v2/api-keys"
    try:
        logger.info(f"[Confluent] Fetching API keys for service account ID: {service_account_id}")
        response = requests.get(url, auth=(api_key, api_secret))
        response.raise_for_status()
        keys = response.json().get("data", [])
        return [key for key in keys if key.get("owner", {}).get("id") == service_account_id]
    except requests.HTTPError as e:
        logger.error(f"[Confluent] Failed to get API keys: {e.response.text}")
        raise
    except Exception as e:
        logger.exception("[Confluent] Unexpected error while retrieving API keys")
        raise