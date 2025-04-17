import kopf
import logging
import asyncio
import random
from functools import wraps

logging.basicConfig(level=logging.INFO)


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
    return {"message": f"Servicealt '{name}' creation logged."}

@kopf.on.update('jones.com', 'v1', 'servicealts')
def update_servicealt(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Servicealt] Updated: '{name}' in namespace '{namespace}'")
    logger.info(f"ContextLink: {spec.get('contextLink')}, SecretSolution: {spec.get('secretSolution')}")
    return {"message": f"Servicealt '{name}' update logged."}

@kopf.on.delete('jones.com', 'v1', 'servicealts')
def delete_servicealt(spec, name, namespace, logger, **kwargs):
    logger.info(f"[Servicealt] Deleted: '{name}' in namespace '{namespace}'")
    return {"message": f"Servicealt '{name}' deletion logged."}
 # type: ignore

def retry_handler(max_attempts=5, base_delay=2, fatal_delay=60):
    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    kopf.logger.warning(
                        f"[{fn.__name__}] Retry {attempt + 1} failed: {e}. Retrying in {delay:.2f}s."
                    )
                    await asyncio.sleep(delay)
            raise kopf.TemporaryError(
                f"{fn.__name__} failed after {max_attempts} attempts.", delay=fatal_delay
            )
        return wrapper
    return decorator

@kopf.on.create('')
@retry_handler()
async def handle_create(spec, name, namespace, **kwargs):
    # Simulate logic
    kopf.logger.info(f"Creating: {name} in {namespace}")
    await do_something()

@kopf.on.update('')
@retry_handler()
async def handle_update(spec, name, namespace, **kwargs):
    kopf.logger.info(f"Updating: {name} in {namespace}")
    await do_something()

@kopf.on.delete('')
@retry_handler()
async def handle_delete(name, namespace, **kwargs):
    kopf.logger.info(f"Deleting: {name} in {namespace}")
    await do_something()

async def do_something():
    import random
    if random.random() < 0.7:
        raise RuntimeError("Fake error")
    kopf.logger.info("Operation succeeded")

