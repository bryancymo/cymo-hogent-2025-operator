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
CONFLUENT_SECRET_NAME = "confluent2-credentials"
SECRET_TYPE_OPAQUE = "Opaque"
CONFIG_LOADED = False
cluster_id = "lkc-vv08z0"
env_id = "env-jzm58p"
organization_id = "0276e553-ed87-4924-83ec-0edb2f363c39"

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

# Servicealt
@kopf.on.create('jones.com', 'v1', 'servicealts')
def create_servicealt(spec, name, namespace, logger, meta, patch, **kwargs):
    # Check if the resource is being deleted
    if meta.get('deletionTimestamp'):
        logger.info(f"[Servicealt] Resource '{name}' is being deleted, skipping creation")
        return

    logger.info(f"[Servicealt] Created: '{name}' in namespace '{namespace}'")
    logger.info(f"ContextLink: {spec.get('contextLink')}, SecretSolution: {spec.get('secretSolution')}")

    retry = kwargs.get("retry", 0)
    if retry > 0:
        capped_retry = min(retry, 30)
        delay = min(2 ** capped_retry + random.uniform(0, 5), 120)
        logger.warning(f"[Servicealt] Retry #{retry} - delaying for {delay:.2f} seconds.")
        raise kopf.TemporaryError(f"[Servicealt] Temporary error: Retry #{retry}", delay=delay)

    sa_name = f"operator2-hogent-{name}"
    secret_name = f"confluent2-{sa_name}-credentials"

    # Initial status update
    patch.status['state'] = 'Processing'
    patch.status['message'] = 'Starting service account creation'

    try:
        mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace=NAMESPACE_ARGOCD)

        existing_sa = get_confluent_service_account_by_name(sa_name, mgmt_api_key, mgmt_api_secret)
        sa_id = existing_sa['id'] if existing_sa else None

        try:
            k8s_secret = v1.read_namespaced_secret(secret_name, namespace)
            secret_exists = True
            sa_id_from_secret = base64.b64decode(k8s_secret.data['SERVICE_ACCOUNT_ID']).decode("utf-8") \
                if k8s_secret.data and 'SERVICE_ACCOUNT_ID' in k8s_secret.data else None

            if sa_id and sa_id_from_secret and sa_id != sa_id_from_secret:
                logger.warning(f"[Servicealt] Mismatch: Confluent SA '{sa_name}' has ID '{sa_id}', but K8s secret '{secret_name}' has SA ID '{sa_id_from_secret}'")
                v1.delete_namespaced_secret(secret_name, namespace)
                logger.info(f"[K8S] Mismatched secret '{secret_name}' removed from namespace '{namespace}'")
                secret_exists = False
                sa_id_from_secret = None
        except ApiException as e:
            if e.status != 404:
                raise kopf.TemporaryError(f"Failed to read K8s secret '{secret_name}': {e}", delay=60)
            secret_exists = False
            sa_id_from_secret = None

        if not sa_id:
            try:
                sa_response = create_confluent_service_account(sa_name, f"Service account for {name}", mgmt_api_key, mgmt_api_secret)
                sa_id = sa_response['id']
                logger.info(f"[Confluent] Service account created: ID={sa_id}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 409:
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

        if sa_id: # Ensure we have an SA ID before attempting role assignment
             assign_role_binding(sa_id, mgmt_api_key, mgmt_api_secret, "Operator")
             logger.info(f"[Confluent] Operator role assigned/re-assigned using dynamic function to service account ID: {sa_id}")
        else:
             logger.error(f"[Servicealt] Cannot assign role, service account ID is not available for '{name}'")


        existing_keys = get_confluent_api_keys_for_service_account(sa_id, cluster_id, mgmt_api_key, mgmt_api_secret)

        if existing_keys and secret_exists:
            patch.status['state'] = 'Ready'
            patch.status['message'] = f"Using existing Confluent API key and Kubernetes secret for '{name}'."
            patch.status['serviceAccountId'] = sa_id
            patch.status['credentialsSecretRef'] = {'name': secret_name, 'namespace': namespace}

        if not existing_keys:
            api_key_data = create_confluent_api_key(sa_id, sa_name, mgmt_api_key, mgmt_api_secret)
            api_key_value = api_key_data['id']
            api_secret_value = api_key_data['secret']
            logger.info(f"[Confluent] New API key created for service account {sa_id}")
            
            try:
                create_k8s_secret(namespace, secret_name, api_key_value, api_secret_value, sa_id)
                logger.info(f"[K8S] Secret '{secret_name}' created/updated in namespace '{namespace}'")
            except Exception as e:
                logger.error(f"[K8S] Failed to create/update secret '{secret_name}': {e}")
                raise kopf.TemporaryError(f"[K8S] Temporary error creating/updating secret '{secret_name}': {e}", delay=60)

        patch.status['state'] = 'Ready'
        patch.status['message'] = 'Confluent Service Account and API Key provisioned; credentials stored in secret.'
        patch.status['serviceAccountId'] = sa_id
        patch.status['credentialsSecretRef'] = {'name': secret_name, 'namespace': namespace}
        logger.info(f"Servicealt '{name}' processed; credentials stored in secret '{secret_name}'.")

    except Exception as e:
        logger.error(f"[Servicealt] Error occurred during processing of '{name}': {str(e)}\n{traceback.format_exc()}")
        if not isinstance(patch.status, dict):
             patch.status = {}
        patch.status['state'] = 'Failed'
        patch.status['message'] = f'Operation failed for {name}: {str(e)}'
        if isinstance(e, kopf.PermanentError):
            raise
        raise kopf.TemporaryError(f"[Servicealt] Temporary error processing '{name}': {e}", delay=60)

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
        mgmt_api_key, mgmt_api_secret = get_confluent_credentials(namespace=NAMESPACE_ARGOCD)
        
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

# Confluent helpers
def get_confluent_credentials(namespace=NAMESPACE_ARGOCD):
    try:
        logger.info(f"[Confluent] Loading credentials from namespace '{namespace}'")
        secret = v1.read_namespaced_secret(CONFLUENT_SECRET_NAME, namespace)
        api_key = base64.b64decode(secret.data['API_KEY']).decode("utf-8")
        api_secret = base64.b64decode(secret.data['API_SECRET']).decode("utf-8")
        logger.info("[Confluent] Credentials loaded successfully")
        return api_key, api_secret
    except client.ApiException as e:
        raise RuntimeError(f"[Confluent] Failed to read secret from namespace '{namespace}': {e}")
    except KeyError as e:
        raise ValueError(f"[Confluent] Missing key in secret: {e}")
    except base64.binascii.Error as e:
        raise ValueError(f"[Confluent] Error decoding base64 data: {e}")
    except Exception as e:
        raise RuntimeError(f"[Confluent] Error fetching credentials: {e}")

def create_k8s_secret(namespace, secret_name, api_key, api_secret, service_account_id):
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
    except requests.HTTPError as e:
        logger.error(f"[Confluent] HTTP error retrieving service account: {e.response.text}")
        return None
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
        logger.error(f"[Confluent] HTTP error retrieving API keys: {e.response.text}")
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

def delete_associated_resources(name, namespace):
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

def assign_role_binding(service_account_id, api_key, api_secret, role_name):
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

def read_role_binding(role_binding_id, api_key, api_secret):
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

def list_role_bindings(organization_id, env_id, cluster_id, api_key, api_secret, principal=None, role_name=None, page_size=None, page_token=None):
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

def delete_role_binding(role_binding_id, api_key, api_secret):
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