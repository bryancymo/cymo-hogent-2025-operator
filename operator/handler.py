import kopf
import asyncio
import random
from functools import wraps

# 1. Retry Handler Decorator
def retry_handler(max_attempts=5, base_delay=2, fatal_delay=60):
    """
    Decorator to wrap any handler function with retry logic.
    Automatically retries on failure with exponential backoff.
    """
    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    # Exponential backoff with jitter
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    kopf.logger.warning(
                        f"[{fn.__name__}] Retry {attempt + 1} failed: {e}. Retrying in {delay:.2f}s."
                    )
                    await asyncio.sleep(delay)
            # If all retries fail, raise a TemporaryError and delay for next retry
            raise kopf.TemporaryError(
                f"{fn.__name__} failed after {max_attempts} attempts.", delay=fatal_delay
            )
        return wrapper
    return decorator

# 2. Define the Event Handlers with Retry Logic

@kopf.on.create('')
@retry_handler()
async def handle_create(spec, name, namespace, **kwargs):
    """
    Handle the creation event of a resource.
    This function will automatically retry on failure.
    """
    kopf.logger.info(f"Creating: {name} in {namespace}")
    await do_something(name)

@kopf.on.update('')
@retry_handler()
async def handle_update(spec, name, namespace, **kwargs):
    """
    Handle the update event of a resource.
    This function will automatically retry on failure.
    """
    kopf.logger.info(f"Updating: {name} in {namespace}")
    await do_something(name)

@kopf.on.delete('')
@retry_handler()
async def handle_delete(name, namespace, **kwargs):
    """
    Handle the deletion event of a resource.
    This function will automatically retry on failure.
    """
    kopf.logger.info(f"Deleting: {name} in {namespace}")
    await do_something(name)

# 3. Simulate Risky Operation (for testing)
async def do_something(name):
    """
    Simulate a risky operation that could fail randomly.
    In real scenarios, this is where your actual logic goes.
    """
    if random.random() < 0.7:  # 70% chance to fail
        raise RuntimeError(f"Simulated failure during operation on {name}")
    kopf.logger.info(f"Operation for {name} succeeded.")
