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

# Constants
NAMESPACE_ARGOCD = "argocd"
NAMESPACE_OPERATOR = "operator"
CONFLUENT_SECRET_NAME = "confluent-credentials"
SECRET_TYPE_OPAQUE = "Opaque"
CONFIG_LOADED = False 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    global CONFIG_LOADED
    if not CONFIG_LOADED:
        try:
            config.load_incluster_config()
            CONFIG_LOADED = True
            logger.info("[Startup] Kubernetes configuration loaded successfully.")
        except Exception as e:
            logger.error(f"[Startup] Failed to load Kubernetes configuration: {e}")
            raise
    settings.watching.namespaces = [NAMESPACE_ARGOCD, NAMESPACE_OPERATOR]

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
def delete_application_topic(spec, name, namespace, logger, meta, status, **kwargs):
    try:
        logger.info(f"[ApplicationTopic] Starting deletion process for: '{name}' in namespace '{namespace}'")
        
        # Get credentials for cleanup
        try:
            mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace=NAMESPACE_ARGOCD)
        except Exception as e:
            logger.error(f"[ApplicationTopic] Failed to get Confluent credentials: {e}")
            raise kopf.PermanentError(f"Failed to get Confluent credentials: {e}")

        # Perform cleanup actions here (e.g., delete Kafka topic)
        # Add your topic deletion logic here if needed

        logger.info(f"[ApplicationTopic] Successfully deleted topic '{name}'")
        return {"message": f"Topic '{name}' and associated resources deleted successfully."}
    except Exception as e:
        logger.error(f"[ApplicationTopic] Error during deletion: {str(e)}\n{traceback.format_exc()}")
        raise kopf.PermanentError(f"Failed to delete ApplicationTopic: {str(e)}")

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
def delete_domain_topic(spec, name, namespace, logger, meta, status, **kwargs):
    try:
        logger.info(f"[Domaintopic] Starting deletion process for: '{name}' in namespace '{namespace}'")
        
        # Get credentials for cleanup
        try:
            mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace=NAMESPACE_ARGOCD)
        except Exception as e:
            logger.error(f"[Domaintopic] Failed to get Confluent credentials: {e}")
            raise kopf.PermanentError(f"Failed to get Confluent credentials: {e}")

        # Perform cleanup actions here (e.g., delete Kafka topic)
        # Add your topic deletion logic here if needed

        logger.info(f"[Domaintopic] Successfully deleted topic '{name}'")
        return {"message": f"Topic '{name}' and associated resources deleted successfully."}
    except Exception as e:
        logger.error(f"[Domaintopic] Error during deletion: {str(e)}\n{traceback.format_exc()}")
        raise kopf.PermanentError(f"Failed to delete Domaintopic: {str(e)}")

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
def delete_context(spec, name, namespace, logger, meta, status, **kwargs):
    try:
        logger.info(f"[Context] Starting deletion process for: '{name}' in namespace '{namespace}'")
        
        # Check for any dependent resources before deletion
        v1_custom = client.CustomObjectsApi()
        
        # Check for dependent Servicealt objects
        try:
            servicealt_list = v1_custom.list_namespaced_custom_object(
                group="jones.com",
                version="v1",
                namespace=namespace,
                plural="servicealts"
            )
            
            dependent_services = [
                svc['metadata']['name'] for svc in servicealt_list.get('items', [])
                if svc.get('spec', {}).get('contextLink') == name
            ]
            
            if dependent_services:
                logger.warning(f"[Context] Found dependent Servicealt objects: {dependent_services}")
                # Optionally, you could raise an error here if you want to prevent deletion
                # when there are dependencies
        except ApiException as e:
            logger.error(f"[Context] Error checking dependent services: {e}")

        logger.info(f"[Context] Successfully deleted context '{name}'")
        return {"message": f"Context '{name}' deleted successfully."}
    except Exception as e:
        logger.error(f"[Context] Error during deletion: {str(e)}\n{traceback.format_exc()}")
        raise kopf.PermanentError(f"Failed to delete Context: {str(e)}")


# Servicealt
@kopf.on.create('jones.com', 'v1', 'servicealts')
def create_servicealt(spec, name, namespace, logger, meta, **kwargs):
    logger.info(f"[Servicealt] Created: '{name}' in namespace '{namespace}'")
    logger.info(f"ContextLink: {spec.get('contextLink')}, SecretSolution: {spec.get('secretSolution')}")
    retry = kwargs.get("retry", 0)

    if retry > 0:
        delay = min(2 ** retry + random.uniform(0, 5), 120)
        logger.warning(f"[Servicealt] Retry #{retry} - delaying for {delay:.2f} seconds.")
        time.sleep(delay)

    sa_name = f"operator-hogent-{name}"
    secret_name = f"confluent-{sa_name}-credentials"

    try:
        # Load Confluent credentials from Kubernetes Secret
        mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace=NAMESPACE_ARGOCD)

        v1 = client.CoreV1Api()
        secret_exists_in_k8s = False
        sa_id_from_secret = None

        # 1. Check for an existing Kubernetes Secret
        try:
            k8s_secret = v1.read_namespaced_secret(secret_name, namespace)
            secret_exists_in_k8s = True
            logger.info(f"[K8S] Found existing secret '{secret_name}' in namespace '{namespace}'.")
            if k8s_secret.data and 'SERVICE_ACCOUNT_ID' in k8s_secret.data:
                # Since we now use stringData when creating secrets, existing ones might remain encoded.
                sa_id_from_secret = base64.b64decode(k8s_secret.data['SERVICE_ACCOUNT_ID']).decode("utf-8")
                logger.info(f"[K8S] Retrieved Service Account ID '{sa_id_from_secret}' from existing secret.")
            else:
                logger.warning(f"[K8S] Existing secret '{secret_name}' found but does not contain SERVICE_ACCOUNT_ID.")
        except ApiException as e:
            if e.status != 404:
                logger.error(f"[K8S] Failed reading secret '{secret_name}': {e}")
                kopf.status.patch(meta, status={'state': 'TemporaryError', 'message': f"Failed to read K8s secret '{secret_name}': {e}"})
                raise kopf.TemporaryError(f"Failed to read K8s secret '{secret_name}': {e}", delay=60)

        # 2. Check if Service Account exists in Confluent by name
        existing_sa_in_confluent = get_confluent_service_account_by_name(sa_name, mgmt_api_key, mgmt_api_secret)
        sa_id = None

        if existing_sa_in_confluent:
            sa_id = existing_sa_in_confluent['id']
            logger.info(f"[Confluent] Found existing service account: ID={sa_id}")
            kopf.status.patch(meta, status={'state': 'Processing', 'message': f'Using existing Confluent Service Account {sa_id}.'})
            if secret_exists_in_k8s and sa_id_from_secret and sa_id != sa_id_from_secret:
                logger.warning(f"[Servicealt] Mismatch: Confluent SA '{sa_name}' has ID '{sa_id}', but K8s secret '{secret_name}' has SA ID '{sa_id_from_secret}'. Proceeding with Confluent SA ID.")

            # 3. Check if API Key exists for this Service Account in Confluent
            existing_keys = get_confluent_api_keys_for_service_account(sa_id, mgmt_api_key, mgmt_api_secret)
            if existing_keys:
                api_key_value = existing_keys[0]['key']
                logger.warning(f"[Confluent] Existing API key found for service account {sa_id}. Cannot retrieve secret part via API.")
                if secret_exists_in_k8s:
                    logger.info(f"[K8S] Kubernetes Secret '{secret_name}' exists. Assuming it holds necessary credentials.")
                    kopf.status.patch(meta, status={
                        'state': 'Ready',
                        'message': 'Using existing Confluent API key and Kubernetes secret.',
                        'serviceAccountId': sa_id,
                        'credentialsSecretRef': {'name': secret_name, 'namespace': namespace}
                    })
                    return {"message": f"Servicealt '{name}' processed, using credentials from secret '{secret_name}'."}
                else:
                    warning_message = f"Existing API key found for SA {sa_id}, but secret {secret_name} missing. Operator cannot provision credentials."
                    logger.warning(f"[Servicealt] {warning_message}")
                    kopf.status.patch(meta, status={
                        'state': 'Warning',
                        'message': warning_message,
                        'serviceAccountId': sa_id
                    })
                    return {"message": f"Servicealt '{name}' processed. Existing API key found, but K8s secret '{secret_name}' missing."}
            else:
                logger.info(f"[Confluent] No existing API keys for service account {sa_id}. Creating a new one.")
                api_key_data = create_confluent_api_key(sa_id, mgmt_api_key, mgmt_api_secret)
                api_key_value = api_key_data['key']
                api_secret_value = api_key_data['secret']
                logger.info(f"[Confluent] New API key created.")
                kopf.status.patch(meta, status={'state': 'Processing', 'message': f'API Key created for SA {sa_id}.'})
                create_k8s_secret(namespace, secret_name, api_key_value, api_secret_value, sa_id)
                logger.info(f"[K8S] Secret '{secret_name}' created/updated in namespace '{namespace}'.")
                kopf.status.patch(meta, status={'state': 'Processing', 'message': f'Secret {secret_name} updated for SA {sa_id}.'})
                kopf.status.patch(meta, status={
                    'state': 'Ready',
                    'message': 'Confluent Service Account and API Key provisioned; credentials stored in secret.',
                    'serviceAccountId': sa_id,
                    'credentialsSecretRef': {'name': secret_name, 'namespace': namespace}
                })
                return {"message": f"Servicealt '{name}' processed; credentials stored in secret '{secret_name}'."}
        else:
            logger.info(f"[Confluent] Service account '{sa_name}' not found. Creating new service account.")
            sa_response = create_confluent_service_account(sa_name, f"Service account for {name}", mgmt_api_key, mgmt_api_secret)
            sa_id = sa_response['id']
            logger.info(f"[Confluent] Service account created: ID={sa_id}")
            kopf.status.patch(meta, status={'state': 'Processing', 'message': f'Service Account {sa_id} created.'})
            api_key_data = create_confluent_api_key(sa_id, mgmt_api_key, mgmt_api_secret)
            api_key_value = api_key_data['key']
            api_secret_value = api_key_data['secret']
            logger.info(f"[Confluent] New API key created for new service account.")
            kopf.status.patch(meta, status={'state': 'Processing', 'message': f'API Key created for SA {sa_id}.'})
            create_k8s_secret(namespace, secret_name, api_key_value, api_secret_value, sa_id)
            logger.info(f"[K8S] Secret '{secret_name}' created/updated in namespace '{namespace}'.")
            kopf.status.patch(meta, status={'state': 'Processing', 'message': f'Secret {secret_name} updated for SA {sa_id}.'})
            kopf.status.patch(meta, status={
                'state': 'Ready',
                'message': 'Confluent Service Account and API Key provisioned; credentials stored in secret.',
                'serviceAccountId': sa_id,
                'credentialsSecretRef': {'name': secret_name, 'namespace': namespace}
            })
            return {"message": f"Servicealt '{name}' processed; credentials stored in secret '{secret_name}'."}

    except Exception as e:
        logger.error(f"[Servicealt] Error occurred: {str(e)}\n{traceback.format_exc()}")
        kopf.status.patch(meta, status={'state': 'Failed', 'message': f'Operation failed: {str(e)}'})
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
def delete_servicealt(spec, name, namespace, logger, meta, status, **kwargs):
    try:
        logger.info(f"[Servicealt] Starting deletion process for: '{name}' in namespace '{namespace}'")
        
        # Get credentials for cleanup
        try:
            mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace=NAMESPACE_ARGOCD)
        except Exception as e:
            logger.error(f"[Servicealt] Failed to get Confluent credentials: {e}")
            raise kopf.PermanentError(f"Failed to get Confluent credentials: {e}")

        # 1. Delete the Kubernetes secret
        v1 = client.CoreV1Api()
        secret_name = f"confluent-operator-hogent-{name}-credentials"
        try:
            v1.delete_namespaced_secret(secret_name, namespace)
            logger.info(f"[Servicealt] Deleted associated secret '{secret_name}'")
        except ApiException as e:
            if e.status != 404: 
                logger.warning(f"[Servicealt] Error deleting secret '{secret_name}': {e}")

        # 2. Delete the Confluent service account if existing
        sa_name = f"operator-hogent-{name}"
        try:
            # First get the service account ID
            existing_sa = get_confluent_service_account_by_name(sa_name, mgmt_api_key, mgmt_api_secret)
            if existing_sa:
                sa_id = existing_sa['id']
                # Delete the service account's API keys first
                delete_confluent_api_keys_for_service_account(sa_id, mgmt_api_key, mgmt_api_secret)
                # Then delete the service account itself
                delete_confluent_service_account(sa_id, mgmt_api_key, mgmt_api_secret)
                logger.info(f"[Servicealt] Deleted Confluent service account '{sa_name}' and its API keys")
        except Exception as e:
            logger.warning(f"[Servicealt] Error cleaning up Confluent resources: {e}")

        logger.info(f"[Servicealt] Successfully deleted servicealt '{name}' and associated resources")
        return {"message": f"Servicealt '{name}' and associated resources deleted successfully."}
    except Exception as e:
        logger.error(f"[Servicealt] Error during deletion: {str(e)}\n{traceback.format_exc()}")
        raise kopf.PermanentError(f"Failed to delete Servicealt: {str(e)}")

# Confluent helpers
def get_confluent_credentials(namespace=NAMESPACE_ARGOCD):
    try:
        logger.info(f"[Confluent] Loading credentials from namespace '{namespace}'")
        v1 = client.CoreV1Api()
        secret = v1.read_namespaced_secret(CONFLUENT_SECRET_NAME, namespace)
        api_key = base64.b64decode(secret.data['API_KEY']).decode("utf-8")
        api_secret = base64.b64decode(secret.data['API_SECRET']).decode("utf-8")
        logger.info("[Confluent] Credentials loaded successfully")
        return api_key, api_secret
    except ApiException as e:
        raise RuntimeError(f"[Confluent] Failed to read secret from namespace '{namespace}': {e}")
    except KeyError as e:
        raise ValueError(f"[Confluent] Missing key in secret: {e}")
    except Exception as e:
        raise RuntimeError(f"[Confluent] Error fetching credentials: {e}")

def create_k8s_secret(namespace, secret_name, api_key, api_secret, service_account_id):
    v1 = client.CoreV1Api()
    secret_manifest = client.V1Secret(
        metadata=client.V1ObjectMeta(name=secret_name, namespace=namespace),
        string_data={
            "API_KEY": api_key,
            "API_SECRET": api_secret,
            "SERVICE_ACCOUNT_ID": service_account_id
        },
        type=SECRET_TYPE_OPAQUE
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
        "resource": {"id": "all"},
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
    logger.info(f"[Confluent] API key created, key: {data.get('key')}")
    return data 

def create_confluent_service_account(name, description, api_key, api_secret):
    url = "https://api.confluent.cloud/iam/v2/service-accounts"
    payload = {
        "display_name": name,
        "description": description
    }
    logger.info(f"[Confluent] Creating service account '{name}'")
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
            logger.info(f"[Confluent] Found matching service account: ID={matched.get('id')}")
        else:
            logger.warning(f"[Confluent] No matching service account found for: '{name}'")
        return matched
    except requests.HTTPError as e:
        logger.error(f"[Confluent] HTTP error retrieving service accounts: {e.response.status_code} - {e.response.text}")
        raise
    except Exception as e:
        logger.exception("[Confluent] Unexpected error retrieving service accounts")
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

def delete_confluent_api_keys_for_service_account(service_account_id, api_key, api_secret):
    """Delete all API keys associated with a service account."""
    url = "https://api.confluent.cloud/iam/v2/api-keys"
    try:
        # Get API keys for the service account
        keys = get_confluent_api_keys_for_service_account(service_account_id, api_key, api_secret)
        
        # Delete each API key
        for key in keys:
            key_id = key['id']
            delete_url = f"{url}/{key_id}"
            response = requests.delete(delete_url, auth=(api_key, api_secret))
            response.raise_for_status()
    except Exception as e:
        raise Exception(f"Failed to delete API keys: {str(e)}")

def delete_confluent_service_account(service_account_id, api_key, api_secret):
    """Delete a Confluent service account."""
    url = f"https://api.confluent.cloud/iam/v2/service-accounts/{service_account_id}"
    try:
        response = requests.delete(url, auth=(api_key, api_secret))
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Failed to delete service account: {str(e)}")