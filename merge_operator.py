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
CLOUD_KEY_SECRET = "confluent2-credentials"
SECRET_TYPE_OPAQUE = "Opaque"
CONFIG_LOADED = False
cluster_id = "lkc-vv08z0"
env_id = "env-jzm58p"
organization_id = "0276e553-ed87-4924-83ec-0edb2f363c39"
rest_endpoint = "https://pkc-619z3.us-east1.gcp.confluent.cloud"

# Global variable for Kubernetes client
v1 = None

# Increase logging level to DEBUG
# Options include: logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    global CONFIG_LOADED, v1
    if not CONFIG_LOADED:
        try:
            config.load_incluster_config()
            CONFIG_LOADED = True
            logger.debug("[Startup] Attempting to load Kubernetes configuration.")
            logger.info("[Startup] Kubernetes configuration loaded successfully.")
            # Initialize the CoreV1Api client
            v1 = client.CoreV1Api()
        except Exception as e:
            logger.error(f"[Startup] Failed to load Kubernetes configuration: {e}")
            raise
    settings.watching.namespaces = [NAMESPACE_ARGOCD, NAMESPACE_OPERATOR]


#ApplicationTopic
@kopf.on.create('jones.com', 'v1', 'applicationtopics')
def create_applicationtopic(spec, name, namespace, status, **kwargs):
    logger.info(f"[ApplicationTopic] Created: '{name}' in namespace '{namespace}'")
    retry = kwargs.get("retry", 0)

    topic_name = spec.get("name", name)
    partitions = spec.get("partitions", 1)
    config = spec.get("config", {})

    def run():
        try:
            service_name = name.replace('-applicationtopic', '')            
            secret_name = f"confluent2-operator2-hogent-{service_name}-credentials"
            api_key, api_secret = get_confluent_credentials(secret_name, namespace=namespace)
            logger.info(f"[Confluent] Retrieved credentials for topic '{topic_name}'")
            
            # Check if topic already exists
            if check_topic_exists(topic_name, api_key, api_secret, cluster_id, rest_endpoint, logger):
                logger.info(f"[Confluent] Topic '{topic_name}' already exists, skipping creation")
                return {'status': {'create_applicationtopic': {'message': f"Topic '{topic_name}' already exists."}}}
            
            create_confluent_topic(topic_name, partitions, config, api_key, api_secret, cluster_id, rest_endpoint, logger)
            logger.info(f"[Confluent] Kafka topic '{topic_name}' created successfully")
            return {'status': {'create_applicationtopic': {'message': f"Topic '{topic_name}' created successfully."}}}
        except Exception as e:
            logger.error(f"[Confluent] Error creating topic '{topic_name}': {e}")
            # Raise a TemporaryError to trigger retry
            raise kopf.TemporaryError(f"Failed to create topic: {str(e)}", delay=30)

    try:
        result = run()
        return result
    except kopf.TemporaryError:
        raise
    except Exception as e:
        return {'status': {'create_applicationtopic': {'message': f"Failed to create topic: {str(e)}"}}}

# Servicealt

@kopf.on.create('jones.com', 'v1', 'servicealts')
def create_servicealt(spec, name, namespace, logger, meta, patch, **kwargs):
    # Check if the resource is being deleted
    if meta.get('deletionTimestamp'):
        logger.info(f"[Servicealt] Resource '{name}' is being deleted, skipping creation")
        return

    logger.info(f"[Servicealt] Processing creation for: '{name}' in namespace '{namespace}'")
    logger.info(f"[Servicealt] ContextLink: {spec.get('contextLink')}, SecretSolution: {spec.get('secretSolution')}")

    retry = kwargs.get("retry", 0)
    if retry > 0:
        delay = 30  # Flat 30-second retry duration
        logger.info(f"[Servicealt] Retry attempt #{retry} for '{name}'. Setting delay to a flat {delay} seconds.")
        logger.warning(f"[Servicealt] Retry #{retry} - delaying for {delay:.2f} seconds.")
        raise kopf.TemporaryError(f"[Servicealt] Temporary error: Retry #{retry}. Will retry in {delay}s.", delay=delay)

    sa_name = f"operator2-hogent-{name}"
    secret_name = f"confluent2-{sa_name}-credentials"

    # Initial status update
    logger.info(f"[Servicealt] Setting initial status to 'Processing' for '{name}'.")
    patch.status['state'] = 'Processing'
    patch.status['message'] = 'Starting service account creation'

    try:
        logger.info(f"[Servicealt] Attempting to get Confluent credentials for '{name}'.")
        mgmt_api_key, mgmt_api_secret = get_confluent_credentials(CLOUD_KEY_SECRET, namespace=NAMESPACE_ARGOCD)
        logger.info(f"[Servicealt] Successfully retrieved Confluent credentials for '{name}'.")

        logger.info(f"[Servicealt] Checking for existing Confluent service account '{sa_name}' for '{name}'.")
        existing_sa = get_confluent_service_account_by_name(sa_name, mgmt_api_key, mgmt_api_secret)
        sa_id = existing_sa['id'] if existing_sa else None
        if sa_id:
            logger.info(f"[Servicealt] Found existing Confluent service account '{sa_name}' with ID '{sa_id}' for '{name}'.")
        else:
            logger.info(f"[Servicealt] No existing Confluent service account found for '{sa_name}' for '{name}'. Will create.")

        try:
            logger.info(f"[Servicealt] Checking for K8s secret '{secret_name}' in namespace '{namespace}' for '{name}'.")
            k8s_secret = v1.read_namespaced_secret(secret_name, namespace)
            secret_exists = True
            logger.info(f"[Servicealt] K8s secret '{secret_name}' exists for '{name}'.")
            sa_id_from_secret = base64.b64decode(k8s_secret.data['SERVICE_ACCOUNT_ID']).decode("utf-8") \
                if k8s_secret.data and 'SERVICE_ACCOUNT_ID' in k8s_secret.data else None

            if sa_id and sa_id_from_secret and sa_id != sa_id_from_secret:
                logger.warning(f"[Servicealt] Mismatch: Confluent SA '{sa_name}' ID '{sa_id}' vs K8s secret SA ID '{sa_id_from_secret}' for '{name}'. Deleting K8s secret.")
                v1.delete_namespaced_secret(secret_name, namespace)
                logger.info(f"[K8S] Mismatched secret '{secret_name}' removed from namespace '{namespace}' for '{name}'.")
                secret_exists = False
                sa_id_from_secret = None
        except ApiException as e:
            if e.status != 404:
                logger.error(f"[Servicealt] Error reading K8s secret '{secret_name}' for '{name}': {e}. Raising temporary error.")
                raise kopf.TemporaryError(f"Failed to read K8s secret '{secret_name}': {e}", delay=60)
            logger.info(f"[Servicealt] K8s secret '{secret_name}' not found for '{name}'.")
            secret_exists = False
            sa_id_from_secret = None

        if not sa_id:
            logger.info(f"[Servicealt] Creating Confluent service account '{sa_name}' for '{name}'.")
            try:
                sa_response = create_confluent_service_account(sa_name, f"Service account for {name}", mgmt_api_key, mgmt_api_secret)
                sa_id = sa_response['id']
                logger.info(f"[Confluent] Service account '{sa_name}' created with ID: {sa_id} for '{name}'.")
            except requests.exceptions.HTTPError as e:
                logger.error(f"[Servicealt] HTTPError creating Confluent SA '{sa_name}' for '{name}': {e.response.text if e.response else e}")
                if e.response is not None and e.response.status_code == 409:
                    logger.warning(f"[Servicealt] Confluent SA '{sa_name}' creation returned 409 (conflict) for '{name}'. Attempting to find existing SA.")
                    for attempt in range(3):
                        time.sleep(2 ** attempt)
                        existing_sa_retry = get_confluent_service_account_by_name(sa_name, mgmt_api_key, mgmt_api_secret)
                        if existing_sa_retry:
                            sa_id = existing_sa_retry['id']
                            logger.info(f"[Servicealt] Found existing SA '{sa_name}' with ID '{sa_id}' after 409 retry for '{name}'.")
                            break
                    if not sa_id:
                        logger.error(f"[Servicealt] Failed to find existing SA '{sa_name}' after 409 and retries for '{name}'. Raising exception.")
                        raise Exception(f"Service account {sa_name} creation failed with 409 but could not find existing account after retries")
                else:
                    raise

        if sa_id:
            logger.info(f"[Servicealt] Assigning 'CloudClusterAdmin' role to Confluent SA ID '{sa_id}' for '{name}'.")
            assign_role_binding(sa_id, mgmt_api_key, mgmt_api_secret, "CloudClusterAdmin")
            logger.info(f"[Confluent] CloudClusterAdmin role assigned/re-assigned to service account ID: {sa_id} for '{name}'.")
            # Add delay to allow role propagation
            logger.info(f"[Servicealt] Waiting 30 seconds for role propagation...")
            time.sleep(30)
        else:
            logger.error(f"[Servicealt] Cannot assign role for '{name}', Confluent service account ID is not available.")

        logger.info(f"[Servicealt] Getting existing Confluent API keys for SA ID '{sa_id}', cluster '{cluster_id}' for '{name}'.")
        existing_keys = get_confluent_api_keys_for_service_account(sa_id, cluster_id, mgmt_api_key, mgmt_api_secret)

        if existing_keys and secret_exists:
            logger.info(f"[Servicealt] Using existing Confluent API key and K8s secret for '{name}'. Setting status to 'Ready'.")
            patch.status['state'] = 'Ready'
            patch.status['message'] = f"Using existing Confluent API key and Kubernetes secret for '{name}'."
            patch.status['serviceAccountId'] = sa_id
            patch.status['credentialsSecretRef'] = {'name': secret_name, 'namespace': namespace}
        elif not existing_keys:
            logger.info(f"[Servicealt] No existing Confluent API keys found for SA ID '{sa_id}' for '{name}'. Creating new API key.")
            api_key_data = create_confluent_api_key(sa_id, sa_name, mgmt_api_key, mgmt_api_secret)
            api_key_value = api_key_data['id']
            api_secret_value = api_key_data['secret']
            logger.info(f"[Confluent] New API key created for service account {sa_id} for '{name}'.")
            
            try:
                logger.info(f"[Servicealt] Creating/updating K8s secret '{secret_name}' in '{namespace}' for '{name}'.")
                create_k8s_secret(namespace, secret_name, api_key_value, api_secret_value, sa_id)
                logger.info(f"[K8S] Secret '{secret_name}' created/updated in namespace '{namespace}' for '{name}'.")
            except Exception as e:
                logger.error(f"[K8S] Failed to create/update K8s secret '{secret_name}' for '{name}': {e}")
                raise kopf.TemporaryError(f"[K8S] Temporary error creating/updating secret '{secret_name}': {e}", delay=60)

        # Create ApplicationTopic after service account is ready
        if sa_id:
            create_application_topic_for_service(name, namespace, logger)

        # Final status update
        if sa_id:
            logger.info(f"[Servicealt] Finalizing status to 'Ready' for '{name}'.")
            patch.status['state'] = 'Ready'
            patch.status['message'] = 'Confluent Service Account and API Key provisioned; credentials stored in secret.'
            patch.status['serviceAccountId'] = sa_id
            patch.status['credentialsSecretRef'] = {'name': secret_name, 'namespace': namespace}
            logger.info(f"[Servicealt] Successfully processed '{name}'; credentials stored in secret '{secret_name}'.")
        else:
            logger.error(f"[Servicealt] Processing for '{name}' reached end without a valid sa_id. Setting status to 'Failed'.")
            patch.status['state'] = 'Failed'
            patch.status['message'] = f'Operation failed for {name} due to missing service account ID before finalization.'

    except kopf.TemporaryError:
        raise
    except Exception as e:
        logger.error(f"[Servicealt] Unhandled error occurred during processing of '{name}': {str(e)}\n{traceback.format_exc()}")
        if not isinstance(patch.status, dict):
            patch.status = {}
        patch.status['state'] = 'Failed'
        patch.status['message'] = f'Operation failed for {name}: {str(e)}'
        if isinstance(e, kopf.PermanentError):
            raise
        raise kopf.TemporaryError(f"[Servicealt] Unhandled temporary error processing '{name}': {e}", delay=60)

@kopf.on.update('jones.com', 'v1', 'servicealts')
def update_servicealt(spec, name, namespace, logger, patch, **kwargs):
    logger.info(f"[Servicealt] Updated: '{name}' in namespace '{namespace}'")
    logger.info(f"ContextLink: {spec.get('contextLink')}, SecretSolution: {spec.get('secretSolution')}")
    patch.status['state'] = 'Updated'
    patch.status['message'] = f"Service '{name}' updated with new context and secret solution."

@kopf.on.delete('jones.com', 'v1', 'servicealts')
def delete_servicealt(spec, name, namespace, logger, patch, **kwargs):
    logger.info(f"[Servicealt] Deleting: '{name}' in namespace '{namespace}'")
    
    # Update status to indicate deletion is in progress
    patch.status['state'] = 'Deleting'
    patch.status['message'] = 'Cleaning up resources'
    
    try:
        # Get Confluent credentials
        mgmt_api_key, mgmt_api_secret = get_confluent_credentials(CLOUD_KEY_SECRET, namespace=NAMESPACE_ARGOCD)
        
        # Construct service account name
        sa_name = f"operator2-hogent-{name}"
        
        # Get the service account ID from the secret if it exists
        secret_name = f"confluent2-{sa_name}-credentials"
        try:
            k8s_secret = v1.read_namespaced_secret(secret_name, namespace)
            service_account_id = base64.b64decode(k8s_secret.data['SERVICE_ACCOUNT_ID']).decode("utf-8")
            
            # Delete API keys first
            try:
                delete_confluent_api_keys_for_service_account(service_account_id, cluster_id, mgmt_api_key, mgmt_api_secret)
                logger.info(f"[Confluent] Deleted API keys for service account ID: {service_account_id}")
            except Exception as e:
                logger.error(f"[Confluent] Error deleting API keys: {e}")
            
            # Delete service account
            try:
                delete_confluent_service_account(service_account_id, mgmt_api_key, mgmt_api_secret)
                logger.info(f"[Confluent] Deleted service account ID: {service_account_id}")
            except Exception as e:
                logger.error(f"[Confluent] Error deleting service account: {e}")
                
        except ApiException as e:
            if e.status != 404:
                logger.error(f"[K8S] Error reading secret '{secret_name}': {e}")
        
        # Delete Kubernetes secret
        try:
            v1.delete_namespaced_secret(secret_name, namespace)
            logger.info(f"[K8S] Deleted secret '{secret_name}' from namespace '{namespace}'")
        except ApiException as e:
            if e.status != 404:
                logger.error(f"[K8S] Error deleting secret '{secret_name}': {e}")
        
        # Delete any other associated resources
        delete_associated_resources(name, namespace)
        
        logger.info(f"[Servicealt] Successfully cleaned up resources for '{name}'")
        
    except Exception as e:
        logger.error(f"[Servicealt] Error during cleanup of '{name}': {str(e)}\n{traceback.format_exc()}")
        raise kopf.TemporaryError(f"[Servicealt] Error during cleanup: {e}", delay=60)




def create_application_topic_for_service(name, namespace, logger):
    """
    Creates an ApplicationTopic for a given service.
    
    Args:
        name (str): The name of the service
        namespace (str): The Kubernetes namespace
        logger: Logger instance for logging
        
    Returns:
        bool: True if creation was successful, False otherwise
    """
    logger.info(f"[Servicealt] Creating ApplicationTopic for '{name}'.")
    try:
        # Create ApplicationTopic manifest
        application_topic = {
            "apiVersion": "jones.com/v1",
            "kind": "ApplicationTopic",
            "metadata": {
                "name": f"{name}-applicationtopic",
                "namespace": namespace
            },
            "spec": {
                "name": f"{name}-applicationtopic",
                "partitions": 1,
                "config": {
                    "retentionMs": 604800000,  # 7 days in milliseconds
                    "cleanupPolicy": "delete",
                    "replicationFactor": 3
                },
                "consumers": []
            }
        }

        # Create the ApplicationTopic using the Kubernetes API
        custom_api = client.CustomObjectsApi()
        custom_api.create_namespaced_custom_object(
            group="jones.com",
            version="v1",
            namespace=namespace,
            plural="applicationtopics",
            body=application_topic
        )
        logger.info(f"[Servicealt] Successfully created ApplicationTopic '{name}-topic' for '{name}'.")
        return True
    except Exception as e:
        logger.error(f"[Servicealt] Failed to create ApplicationTopic for '{name}': {e}")
        return False

# Confluent helpers
def get_confluent_credentials(secret_name, namespace=NAMESPACE_ARGOCD): #JB
    """Retrieve Confluent credentials from a specified Kubernetes secret."""
    try:
        logger.info(f"[Confluent] Loading credentials from secret '{secret_name}' in namespace '{namespace}'")
        secret = v1.read_namespaced_secret(secret_name, namespace)
        api_key = base64.b64decode(secret.data['API_KEY']).decode("utf-8")
        api_secret = base64.b64decode(secret.data['API_SECRET']).decode("utf-8")
        logger.info(f"[Confluent] Credentials from secret '{secret_name}' loaded successfully")
        return api_key, api_secret
    except client.ApiException as e:
        raise RuntimeError(f"[Confluent] Failed to read secret '{secret_name}' from namespace '{namespace}': {e}")
    except KeyError as e:
        raise ValueError(f"[Confluent] Missing key {e} in secret '{secret_name}'")
    except base64.binascii.Error as e:
        raise ValueError(f"[Confluent] Error decoding base64 data from secret '{secret_name}': {e}")
    except Exception as e:
        raise RuntimeError(f"[Confluent] Error fetching credentials from secret '{secret_name}': {e}")


def create_k8s_secret(namespace, secret_name, api_key, api_secret, service_account_id): #JB
    """Create a Kubernetes secret with Confluent credentials."""
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

def create_confluent_api_key(service_account_id, service_account_name, api_key, api_secret): #JB    
    """Create a Confluent API key for a service account."""
    url = "https://api.confluent.cloud/iam/v2/api-keys"
    payload = {
        "spec": {
            "display_name": f"API Key for {service_account_name}",
            "description": "Empty",
            "owner": {"id": service_account_id},
            "resource": {"id": cluster_id, "type": "kafka-cluster"}
        }
    }
    logger.info(f"[Confluent] Creating API key for service account ID: {service_account_id} and cluster ID: {cluster_id}")
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

def create_confluent_service_account(name, description, api_key, api_secret): #JB
    """Create a Confluent service account."""
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

def get_confluent_service_account_by_name(name, api_key, api_secret): #JB
    """Retrieve a Confluent service account by name."""
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
    except requests.HTTPError as e:
        logger.error(f"[Confluent] HTTP error retrieving service account: {e.response.text}")
        return None
    except Exception as e:
        logger.error(f"[Confluent] Error retrieving service account: {e}")
        return None

def get_confluent_api_keys_for_service_account(service_account_id, cluster_id, api_key, api_secret): #JB
    """Retrieve all API keys for a specific service account."""
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
        logger.error(f"[Confluent] HTTP error retrieving API keys: {e.response.text}")
        raise
    except Exception as e:
        logger.exception("[Confluent] Unexpected error while retrieving API keys")
        raise

def delete_confluent_api_keys_for_service_account(service_account_id, cluster_id, api_key, api_secret): #JB
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

def delete_confluent_service_account(service_account_id, api_key, api_secret): #JB
    """Delete a Confluent service account."""
    url = f"https://api.confluent.cloud/iam/v2/service-accounts/{service_account_id}"
    try:
        response = requests.delete(url, auth=(api_key, api_secret))
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Failed to delete service account: {str(e)}")

def delete_associated_resources(name, namespace): #JB 
    """Delete any associated Kubernetes resources."""
    
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

def delete_confluent_topic(topic_name, api_key, api_secret): #JB
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


#Rolebinding
def assign_role_binding(service_account_id, api_key, api_secret, role_name): #JB
    """Assign a specific role to a service account in the cluster."""
    url = "https://api.confluent.cloud/iam/v2/role-bindings"
    crn_pattern = f"crn://confluent.cloud/organization={organization_id}/environment={env_id}/cloud-cluster={cluster_id}"
    payload = {
        "principal": f"User:{service_account_id}",
        "role_name": role_name,
        "crn_pattern": crn_pattern
    }
    try:
        response = requests.post(url, json=payload, auth=(api_key, api_secret))
        response.raise_for_status()
        logger.info(f"[Confluent] {role_name} role assigned to service account ID: {service_account_id}")
    except requests.HTTPError as e:
        logger.error(f"[Confluent] Failed to assign {role_name} role: {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"[Confluent] Error assigning {role_name} role: {e}")
        raise

def read_role_binding(role_binding_id, api_key, api_secret): #JB
    """Reads a specific role binding by ID."""
    url = f"https://api.confluent.cloud/iam/v2/role-bindings/{role_binding_id}"
    try:
        logger.info(f"[Confluent] Attempting to read role binding with ID: {role_binding_id}")
        response = requests.get(url, auth=(api_key, api_secret))
        response.raise_for_status()
        logger.info(f"[Confluent] Successfully read role binding with ID: {role_binding_id}")
        return response.json() # Return the role binding details
    except requests.HTTPError as e:
        logger.error(f"[Confluent] Failed to read role binding (HTTP error {e.response.status_code}): {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"[Confluent] Error reading role binding: {e}")
        raise

def list_role_bindings(organization_id, env_id, cluster_id, api_key, api_secret, principal=None, role_name=None, page_size=None, page_token=None): #JB
    """Lists role bindings for a specific cluster with optional filters."""
    url = "https://api.confluent.cloud/iam/v2/role-bindings"
    crn_pattern_value = f"crn://confluent.cloud/organization={organization_id}/environment={env_id}/cloud-cluster={cluster_id}"
    params = {
        "crn_pattern": crn_pattern_value
    }

    if principal:
        params["principal"] = principal
    if role_name:
        params["role_name"] = role_name
    if page_size is not None: # Check for None specifically to allow 0 or other integer values if applicable
        params["page_size"] = page_size
    if page_token:
        params["page_token"] = page_token

    try:
        logger.info(f"[Confluent] Attempting to list role bindings with parameters: {params}")
        response = requests.get(url, params=params, auth=(api_key, api_secret))
        response.raise_for_status()
        logger.info(f"[Confluent] Successfully listed role bindings.")
        return response.json()
    except requests.HTTPError as e:
        logger.error(f"[Confluent] Failed to list role bindings (HTTP error {e.response.status_code}): {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"[Confluent] Error listing role bindings: {e}")
        raise

def delete_role_binding(role_binding_id, api_key, api_secret): #JB
    """Deletes a specific role binding by ID."""
    url = f"https://api.confluent.cloud/iam/v2/role-bindings/{role_binding_id}"
    try:
        logger.info(f"[Confluent] Attempting to delete role binding with ID: {role_binding_id}")
        response = requests.delete(url, auth=(api_key, api_secret))
        response.raise_for_status()
        logger.info(f"[Confluent] Successfully deleted role binding with ID: {role_binding_id}")
        if response.status_code == 200:
            return True
        else:
            return f"Deletion response status: {response.status_code}"

    except requests.HTTPError as e:
        logger.error(f"[Confluent] Failed to delete role binding (HTTP error {e.response.status_code}): {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"[Confluent] Error deleting role binding: {e}")
        raise


#Topic
def check_topic_exists(topic_name, api_key, api_secret, cluster_id, rest_endpoint, logger): #RO
    """Check if a topic exists in Confluent Cloud."""
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

def create_confluent_topic(name, partitions, config, api_key, api_secret, cluster_id, rest_endpoint, logger): #RO
    """Create a topic in Confluent Cloud."""
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

    response = None
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
def retry_with_backoff(func, retry_count, logger, error_msg="Operation failed"):
    """
    Retry a function with exponential backoff.
    
    Args:
        func: The function to retry
        retry_count: Current retry attempt
        logger: Logger instance
        error_msg: Message to log on failure
        
    Returns:
        The result of the function call if successful
    """
    max_retries = 5
    if retry_count >= max_retries:
        logger.error(f"{error_msg} after {max_retries} attempts")
        raise Exception(f"{error_msg} after {max_retries} attempts")

    try:
        return func()
    except Exception as e:
        logger.warning(f"Attempt {retry_count + 1} failed: {str(e)}")
        delay = min(30 * (2 ** retry_count), 300)  # Exponential backoff with max 300 seconds
        logger.info(f"Retrying in {delay} seconds...")
        raise kopf.TemporaryError(f"Retry {retry_count + 1}/{max_retries}: {str(e)}", delay=delay)
