import kopf
import asyncio
import random
from functools import wraps

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

