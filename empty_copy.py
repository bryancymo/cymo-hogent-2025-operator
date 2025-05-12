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
cluster_id = "lkc-n9z7v3"

# Increase logging level to DEBUG
# Options include: logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    global CONFIG_LOADED
    if not CONFIG_LOADED:
        try:
            config.load_incluster_config()
            CONFIG_LOADED = True
            logger.debug("[Startup] Attempting to load Kubernetes configuration.")
            logger.info("[Startup] Kubernetes configuration loaded successfully.")
        except Exception as e:
            logger.error(f"[Startup] Failed to load Kubernetes configuration: {e}")
            raise
    settings.watching.namespaces = [NAMESPACE_ARGOCD, NAMESPACE_OPERATOR]

# ApplicationTopic
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

        try:
            delete_confluent_topic(name, mgmt_api_key, mgmt_api_secret)
            logger.info(f"[ApplicationTopic] Successfully deleted topic '{name}' from Confluent")
        except TopicNotFoundException:
            logger.info(f"[ApplicationTopic] Topic '{name}' not found in Confluent - proceeding with resource cleanup")
        except Exception as e:
            logger.error(f"[ApplicationTopic] Error deleting topic from Confluent: {e}")
        # Perform cleanup logic
        # Logic soon TM sure sure

        # Delete associated resources
        try:
            delete_associated_resources(name, namespace)
            logger.info(f"[ApplicationTopic] Successfully deleted associated resources for '{name}'")
        except Exception as e:
            logger.error(f"[ApplicationTopic] Failed to delete associated resources: {e}")
            raise kopf.PermanentError(f"Failed to delete associated resources: {e}")

        logger.info(f"[ApplicationTopic] Successfully completed deletion process for topic '{name}'")
        return {"message": f"Topic '{name}' and associated resources deleted successfully."}
    except Exception as e:
        logger.error(f"[ApplicationTopic] Error during deletion: {str(e)}\n{traceback.format_exc()}")
        raise kopf.PermanentError(f"Failed to delete ApplicationTopic: {str(e)}")

# Domaintopic
@kopf.on.delete('jones.com', 'v1', 'domaintopics')
def delete_domain_topic(spec, name, namespace, logger, meta, status, **kwargs):
    try:
        logger.info(f"[Domaintopic] Starting deletion process for: '{name}' in namespace '{namespace}'")
        
        # Get credentials
        try:
            mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace=NAMESPACE_ARGOCD)
        except Exception as e:
            logger.error(f"[Domaintopic] Failed to get Confluent credentials: {e}")
            raise kopf.PermanentError(f"Failed to get Confluent credentials: {e}")

        # Delete topic from Confluent
        try:
            delete_confluent_topic(name, mgmt_api_key, mgmt_api_secret)
            logger.info(f"[DomainTopic] Successfully deleted topic '{name}' from Confluent")
        except TopicNotFoundException:
            logger.info(f"[DomainTopic] Topic '{name}' not found in Confluent - proceeding with resource cleanup")
        except Exception as e:
            logger.error(f"[DomainTopic] Error deleting topic from Confluent: {e}")
        # Delete  resources
        try:
            delete_associated_resources(name, namespace)
            logger.info(f"[Domaintopic] Successfully deleted associated resources for '{name}'")
        except Exception as e:
            logger.error(f"[Domaintopic] Failed to delete associated resources: {e}")
            raise kopf.PermanentError(f"Failed to delete associated resources: {e}")

        logger.info(f"[Domaintopic] Successfully completed deletion process for topic '{name}'")
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



# Servicealt
@kopf.on.create('jones.com', 'v1', 'servicealts')
def create_servicealt(spec, name, namespace, logger, meta, **kwargs):
    logger.info(f"[Servicealt] Created: '{name}' in namespace '{namespace}'")
    logger.info(f"ContextLink: {spec.get('contextLink')}, SecretSolution: {spec.get('secretSolution')}")
    
    # Handle retry logic
    retry = kwargs.get("retry", 0)
    if retry > 0:
        delay = min(2 ** retry + random.uniform(0, 5), 120)
        logger.warning(f"[Servicealt] Retry #{retry} - delaying for {delay:.2f} seconds.")
        time.sleep(delay)

    sa_name = f"operator-hogent-{name}"
    secret_name = f"confluent-{sa_name}-credentials"
    status = kopf.Status(meta)
    status.state = 'Processing'
    status.message = 'Starting service account creation'

    try:
        # Load Confluent credentials once
        mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace=NAMESPACE_ARGOCD)
        v1 = client.CoreV1Api()

        # Check for existing service account in Confluent
        existing_sa = get_confluent_service_account_by_name(sa_name, mgmt_api_key, mgmt_api_secret)
        sa_id = existing_sa['id'] if existing_sa else None

        # Check for existing Kubernetes Secret
        try:
            k8s_secret = v1.read_namespaced_secret(secret_name, namespace)
            secret_exists = True
            sa_id_from_secret = base64.b64decode(k8s_secret.data['SERVICE_ACCOUNT_ID']).decode("utf-8") if k8s_secret.data and 'SERVICE_ACCOUNT_ID' in k8s_secret.data else None
            
            if sa_id and sa_id_from_secret and sa_id != sa_id_from_secret:
                logger.warning(f"[Servicealt] Mismatch: Confluent SA '{sa_name}' has ID '{sa_id}', but K8s secret '{secret_name}' has SA ID '{sa_id_from_secret}'")
        except ApiException as e:
            if e.status != 404:
                raise kopf.TemporaryError(f"Failed to read K8s secret '{secret_name}': {e}", delay=60)
            secret_exists = False
            sa_id_from_secret = None

        # If service account doesn't exist, create it
        if not sa_id:
            try:
                sa_response = create_confluent_service_account(sa_name, f"Service account for {name}", mgmt_api_key, mgmt_api_secret)
                sa_id = sa_response['id']
                logger.info(f"[Confluent] Service account created: ID={sa_id}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 409:
                    # Handle race condition where SA was created between our check and creation attempt
                    for attempt in range(3):
                        time.sleep(2 ** attempt)
                        existing_sa = get_confluent_service_account_by_name(sa_name, mgmt_api_key, mgmt_api_secret)
                        if existing_sa:
                            sa_id = existing_sa['id']
                            break
                    if not sa_id:
                        raise Exception("Service account creation failed with 409 but could not find existing account after retries")
                else:
                    raise

        # Check for existing API keys
        existing_keys = get_confluent_api_keys_for_service_account(sa_id, cluster_id, mgmt_api_key, mgmt_api_secret)
        
        if existing_keys and secret_exists:
            # We have both API key and secret
            status.state = 'Ready'
            status.message = 'Using existing Confluent API key and Kubernetes secret.'
            status.serviceAccountId = sa_id
            status.credentialsSecretRef = {'name': secret_name, 'namespace': namespace}
            return {"message": f"Servicealt '{name}' processed, using credentials from secret '{secret_name}'."}
        
        # Create new API key if needed
        if not existing_keys:
            api_key_data = create_confluent_api_key(sa_id, sa_name, mgmt_api_key, mgmt_api_secret)
            api_key_value = api_key_data['id']
            api_secret_value = api_key_data['secret']
            logger.info(f"[Confluent] New API key created for service account {sa_id}")
            
            # Create/update Kubernetes secret
            create_k8s_secret(namespace, secret_name, api_key_value, api_secret_value, sa_id)
            logger.info(f"[K8S] Secret '{secret_name}' created/updated in namespace '{namespace}'")
        
        # Update final status
        status.state = 'Ready'
        status.message = 'Confluent Service Account and API Key provisioned; credentials stored in secret.'
        status.serviceAccountId = sa_id
        status.credentialsSecretRef = {'name': secret_name, 'namespace': namespace}
        return {"message": f"Servicealt '{name}' processed; credentials stored in secret '{secret_name}'."}

    except Exception as e:
        logger.error(f"[Servicealt] Error occurred: {str(e)}\n{traceback.format_exc()}")
        status.state = 'Failed'
        status.message = f'Operation failed: {str(e)}'
        if isinstance(e, kopf.PermanentError):
            raise
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
        secret_name = f"confluent-operator-hogent-{name}-credentials"
        try:
            v1 = client.CoreV1Api()
            v1.delete_namespaced_secret(secret_name, namespace)
            logger.info(f"[Servicealt] Deleted associated secret '{secret_name}'")
        except ApiException as e:
            if e.status != 404:  # Ignore if secret doesn't exist
                logger.warning(f"[Servicealt] Error deleting secret '{secret_name}': {e}")

        # 2. Clean up Confluent resources
        sa_name = f"operator-hogent-{name}"
        try:
            # Get the service account
            existing_sa = get_confluent_service_account_by_name(sa_name, mgmt_api_key, mgmt_api_secret)
            if existing_sa:
                sa_id = existing_sa['id']
                # Delete API keys first
                try:
                    delete_confluent_api_keys_for_service_account(sa_id, cluster_id, mgmt_api_key, mgmt_api_secret)
                    logger.info(f"[Servicealt] Deleted API keys for service account '{sa_name}'")
                except Exception as e:
                    logger.error(f"[Servicealt] Error deleting API keys: {e}")

                # Then delete the service account
                #try:
                #    delete_confluent_service_account(sa_id, mgmt_api_key, mgmt_api_secret)
                #    logger.info(f"[Servicealt] Deleted service account '{sa_name}'")
                #except Exception as e:
                #    logger.error(f"[Servicealt] Error deleting service account: {e}")
            else:
                logger.info(f"[Servicealt] No service account found for '{sa_name}'")
        except Exception as e:
            logger.error(f"[Servicealt] Error during Confluent cleanup: {e}")

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

def create_confluent_api_key(service_account_id, service_account_name, api_key, api_secret):
    url = "https://api.confluent.cloud/iam/v2/api-keys"
    payload = {
        "spec": {
            "display_name": f"API Key for {service_account_name}",
            "description": "Empty",
            "owner": {"id": service_account_id},
            "resource": {"id": "lkc-n9z7v3", "type": "kafka-cluster"}
        }
    }
    logger.info(f"[Confluent] Creating API key for service account ID: {service_account_id} and cluster ID: lkc-n9z7v3")
    response = requests.post(url, json=payload, auth=(api_key, api_secret))
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        logger.error(f"[Confluent] Failed to create API key: {e.response.text}")
        raise
    data = response.json()
    logger.debug(f"[Confluent] Full API response: {data}")
    
    # Check for 'id' and 'spec.secret' in the response
    if 'id' not in data or 'spec' not in data or 'secret' not in data['spec']:
        logger.error(f"[Confluent] API response does not contain expected fields. Full response: {data}")
        raise KeyError("API response does not contain expected fields.")
    
    api_key_id = data['id']
    api_key_secret = data['spec']['secret']
    logger.info(f"[Confluent] API key created, ID: {api_key_id}, Secret: {api_key_secret}")
    return {"id": api_key_id, "secret": api_key_secret}

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
    url = "https://api.confluent.cloud/iam/v2/service-accounts"
    params = {
        "display_name": name  # Search By Name
    }
    try:
        response = requests.get(url, auth=(api_key, api_secret), params=params)
        response.raise_for_status()
        accounts = response.json().get("data", [])
        logger.debug(f"[Confluent] Retrieved {len(accounts)} service accounts")
        logger.debug(f"[Confluent] Full response: {response.json()}")
        logger.debug(f"[Confluent] Filter used: {params}")
        
        # Should only get 1 result right???
        matched = accounts[0] if accounts else None
        if matched:
            if matched.get('display_name') != name:
                logger.warning(f"[Confluent] Found service account with different name: expected '{name}', got '{matched.get('display_name')}'")
                return None
            logger.info(f"[Confluent] Found matching service account: ID={matched.get('id')}")
        else:
            logger.warning(f"[Confluent] No matching service account found for: '{name}'")
        return matched
    except Exception as e:
        logger.error(f"[Confluent] Error retrieving service account: {e}")
        return None

def get_confluent_api_keys_for_service_account(service_account_id, cluster_id, api_key, api_secret):
    url = "https://api.confluent.cloud/iam/v2/api-keys"
    params = {
        "spec.owner": service_account_id,
        "spec.resource": cluster_id
    }
    try:
        logger.info(f"[Confluent] Fetching API keys for service account ID: {service_account_id} and cluster ID: {cluster_id}")
        response = requests.get(url, auth=(api_key, api_secret), params=params)
        response.raise_for_status()
        keys = response.json().get("data", [])
        return keys
    except requests.HTTPError as e:
        logger.error(f"[Confluent] Failed to get API keys: {e.response.text}")
        raise
    except Exception as e:
        logger.exception("[Confluent] Unexpected error while retrieving API keys")
        raise

def delete_confluent_api_keys_for_service_account(service_account_id, cluster_id, api_key, api_secret):
    """Delete all API keys associated with a service account."""
    url = "https://api.confluent.cloud/iam/v2/api-keys"
    try:
        # Get API keys for the service account
        keys = get_confluent_api_keys_for_service_account(service_account_id, cluster_id, api_key, api_secret)
        
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
    
    pass

def delete_associated_resources(name, namespace):
    """Delete any associated Kubernetes resources."""
    v1 = client.CoreV1Api()
    
    # Delete configmaps
    try:
        config_map_name = f"{name}-config"
        v1.delete_namespaced_config_map(config_map_name, namespace)
        logger.info(f"Deleted ConfigMap {config_map_name}")
    except ApiException as e:
        if e.status != 404:
            logger.error(f"Error deleting ConfigMap: {e}")

    # Delete secrets
    try:
        secret_name = f"{name}-secret"
        v1.delete_namespaced_secret(secret_name, namespace)
        logger.info(f"Deleted Secret {secret_name}")
    except ApiException as e:
        if e.status != 404:
            logger.error(f"Error deleting Secret: {e}")

class TopicNotFoundException(Exception):
    pass

def delete_confluent_topic(topic_name, api_key, api_secret):
    """Delete a topic from Confluent Cloud."""
    url = f"https://api.confluent.cloud/kafka/v3/clusters/{CLUSTER_ID}/topics/{topic_name}"
    try:
        response = requests.delete(url, auth=(api_key, api_secret))
        if response.status_code == 404:
            raise TopicNotFoundException(f"Topic '{topic_name}' not found")
        response.raise_for_status()
        logger.info(f"[Confluent] Successfully deleted topic '{topic_name}'")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            raise TopicNotFoundException(f"Topic '{topic_name}' not found")
        raise Exception(f"Failed to delete topic: {e.response.text}")
    except Exception as e:
        raise Exception(f"Error deleting topic: {str(e)}")