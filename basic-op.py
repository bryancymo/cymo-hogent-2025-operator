import kopf
import logging

# Configure Kopf logging level if desired (optional)
# kopf.configure(verbose=True) # or DEBUG level
# Or use standard Python logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


# Use multiple decorators for the same handler function
# Watch for creation events in the 'argocd' namespace specifically
@kopf.on.create('jones.com', 'v1',  'contexts', namespace='argocd')
@kopf.on.create('jones.com', 'v1', 'servicealts', namespace='argocd')
@kopf.on.create('jones.com', 'v1', 'domaintopics', namespace='argocd')
@kopf.on.create('jones.com', 'v1', 'applicationtopics', namespace='argocd')
def handle_creation(kind, name, namespace, logger, **kwargs):
    """
    Handles the creation of any of the specified CRDs in the 'argocd' namespace.
    """
    # logger is automatically injected by Kopf
    logger.info(f"Detected creation of new object!")
    logger.info(f"  Kind:      {kind}")
    logger.info(f"  Name:      {name}")
    logger.info(f"  Namespace: {namespace}")

    # You could access the full object via 'body' or 'spec'/'meta' kwargs
    # logger.info(f"  Spec: {kwargs.get('spec')}")

    # No specific action needed other than logging for this basic example
    return {'message': f'Object {kind}/{name} creation detected and logged.'}

# You can add a simple startup handler for confirmation (optional)
@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, logger, **_):
    # Adjust settings dynamically at startup if needed
    # settings.posting.level = logging.INFO
    logger.info("Basic Kopf watcher started successfully.")