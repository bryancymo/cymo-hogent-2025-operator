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
def create_servicealt(spec, name, namespace, logger, meta, **kwargs):
    logger.info(f"[Servicealt] Created: '{name}' in namespace '{namespace}'")
    logger.info(f"ContextLink: {spec.get('contextLink')}, SecretSolution: {spec.get('secretSolution')}")

    retry = kwargs.get("retry", 0)

    # Simple Retry Logic
    if retry > 0:
        delay = min(retry * 10, 120) # Slowly up the retry
        logger.warning(f"[Servicealt] Retry #{retry} - delaying for {delay} seconds.")
        time.sleep(delay)

    sa_name = f"operator-hogent-{name}"
    secret_name = f"confluent-{sa_name}-credentials"

    try:
        # Load Confluent credentials from Kubernetes Secret
        mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace='argocd')

        v1 = client.CoreV1Api()
        secret_exists_in_k8s = False
        sa_id_from_secret = None

        # 1. Check if the corresponding Kubernetes Secret already exists
        try:
            k8s_secret = v1.read_namespaced_secret(secret_name, namespace)
            secret_exists_in_k8s = True
            logger.info(f"[K8S] Found existing secret '{secret_name}' in namespace '{namespace}'.")
            if k8s_secret.data and 'SERVICE_ACCOUNT_ID' in k8s_secret.data:
                 sa_id_from_secret = base64.b64decode(k8s_secret.data['SERVICE_ACCOUNT_ID']).decode("utf-8")
                 logger.info(f"[K8S] Retrieved Service Account ID '{sa_id_from_secret}' from existing secret.")
            else:
                 logger.warning(f"[K8S] Existing secret '{secret_name}' found but does not contain SERVICE_ACCOUNT_ID.")

        except ApiException as e:
            if e.status != 404:
                 logger.error(f"[K8S] Failed to read secret '{secret_name}': {e}")
                 kopf.status.patch(meta, status={'state': 'TemporaryError', 'message': f"Failed to read K8s secret '{secret_name}': {e}"})
                 raise kopf.TemporaryError(f"Failed to read K8s secret '{secret_name}': {e}", delay=60)

        # 2. Check if Service Account already exists in Confluent by name
        existing_sa_in_confluent = get_confluent_service_account_by_name(sa_name, mgmt_api_key, mgmt_api_secret)

        sa_id = None

        if existing_sa_in_confluent:
            sa_id = existing_sa_in_confluent['id']
            logger.info(f"[Confluent] Found existing service account: ID={sa_id}")
            kopf.status.patch(meta, status={'state': 'Processing', 'message': f'Using existing Confluent Service Account {sa_id}.'})

            # Check for mismatch between SA ID in Confluent and SA ID in K8s secret
            if secret_exists_in_k8s and sa_id_from_secret and sa_id != sa_id_from_secret:
                 logger.warning(f"[Servicealt] Mismatch: Confluent SA '{sa_name}' has ID '{sa_id}', but K8s secret '{secret_name}' has SA ID '{sa_id_from_secret}'. Proceeding with Confluent SA ID.")


            # 3. Check if API Key already exists for this Service Account in Confluent
            existing_keys = get_confluent_api_keys_for_service_account(sa_id, mgmt_api_key, mgmt_api_secret)

            if existing_keys:
                api_key_value = existing_keys[0]['key']
                logger.warning(f"[Confluent] Existing API key found for service account {sa_id}. Cannot retrieve the secret part of the key via API.")

                if secret_exists_in_k8s:
                    logger.info(f"[K8S] Corresponding Kubernetes Secret '{secret_name}' already exists. Assuming it contains the necessary credentials.")
                    # Status update: Ready
                    kopf.status.patch(meta, status={
                        'state': 'Ready',
                        'message': 'Using existing Confluent API key and Kubernetes secret.',
                        'serviceAccountId': sa_id,
                        'credentialsSecretRef': {'name': secret_name, 'namespace': namespace}
                    })
                    return {"message": f"Servicealt '{name}' processed, using existing credentials stored in secret '{secret_name}'."}
                else:
                    # Existing Confluent API key found, but no corresponding K8s Secret
                    warning_message = f"Existing API key found for SA {sa_id}, but secret {secret_name} not found. Operator cannot provision credentials."
                    logger.warning(f"[Servicealt] {warning_message}")
                    # Status update: Warning
                    kopf.status.patch(meta, status={
                        'state': 'Warning',
                        'message': warning_message,
                        'serviceAccountId': sa_id
                    })
                    # Return a message and set status to Warning. The user needs to manually provision or delete the Confluent API key.
                    return {"message": f"Servicealt '{name}' processed. Existing API key found, but K8s secret '{secret_name}' missing. Credentials not provisioned by operator."}

            else: # No existing API keys found for the existing SA
                logger.info(f"[Confluent] No existing API keys found for service account {sa_id}. Creating a new one.")
                # Create a new API key
                api_key_data = create_confluent_api_key(sa_id, mgmt_api_key, mgmt_api_secret)
                api_key_value = api_key_data['key']
                api_secret_value = api_key_data['secret']
                logger.info(f"[Confluent] New API key created.")
                # Status update: API Key Created
                kopf.status.patch(meta, status={'state': 'Processing', 'message': f'API Key created for SA {sa_id}.'})

                # Store the new key and secret in a Kubernetes secret
                create_k8s_secret(namespace, secret_name, api_key_value, api_secret_value, sa_id)
                logger.info(f"[K8S] Secret '{secret_name}' created/updated in namespace '{namespace}'.")
                # Status update: Secret Created/Updated
                kopf.status.patch(meta, status={'state': 'Processing', 'message': f'Secret {secret_name} created/updated for SA {sa_id}.'})

                # Final status update: Ready
                kopf.status.patch(meta, status={
                    'state': 'Ready',
                    'message': 'Confluent Service Account and API Key provisioned, credentials stored in secret.',
                    'serviceAccountId': sa_id,
                    'credentialsSecretRef': {'name': secret_name, 'namespace': namespace}
                })
                return {"message": f"Servicealt '{name}' processed, credentials stored in secret '{secret_name}'."}

        else: # Service Account does not exist in Confluent
            logger.info(f"[Confluent] Service account '{sa_name}' not found. Creating...")
            sa_response = create_confluent_service_account(sa_name, f"Service account for {name}", mgmt_api_key, mgmt_api_secret)
            sa_id = sa_response['id']
            logger.info(f"[Confluent] Service account created: ID={sa_id}")
            # Status update: SA Created
            kopf.status.patch(meta, status={'state': 'Processing', 'message': f'Service Account {sa_id} created.'})

            # Create a new API key for the newly created Service Account
            api_key_data = create_confluent_api_key(sa_id, mgmt_api_key, mgmt_api_secret)
            api_key_value = api_key_data['key']
            api_secret_value = api_key_data['secret']
            logger.info(f"[Confluent] New API key created.")
            # Status update: API Key Created
            kopf.status.patch(meta, status={'state': 'Processing', 'message': f'API Key created for SA {sa_id}.'})

            # Store the new key and secret in a Kubernetes secret
            create_k8s_secret(namespace, secret_name, api_key_value, api_secret_value, sa_id)
            logger.info(f"[K8S] Secret '{secret_name}' created/updated in namespace '{namespace}'.")
            # Status update: Secret Created/Updated
            kopf.status.patch(meta, status={'state': 'Processing', 'message': f'Secret {secret_name} created/updated for SA {sa_id}.'})

            # Final status update: Ready
            kopf.status.patch(meta, status={
                'state': 'Ready',
                'message': 'Confluent Service Account and API Key provisioned, credentials stored in secret.',
                'serviceAccountId': sa_id,
                'credentialsSecretRef': {'name': secret_name, 'namespace': namespace}
            })
            return {"message": f"Servicealt '{name}' processed, credentials stored in secret '{secret_name}'."}


    except Exception as e:
        logger.error(f"[Servicealt] Error occurred: {str(e)}\n{traceback.format_exc()}")
        # Status update: Failed
        kopf.status.patch(meta, status={'state': 'Failed', 'message': f'Operation failed: {str(e)}'})
        # Rethrow as TemporaryError to allow Kopf to retry, unless it's a PermanentError
        if isinstance(e, kopf.PermanentError):
            raise
        else:
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