import kopf
import asyncio
import random

@kopf.on.create('myresources')
async def create_fn(spec, **kwargs):
    retry_attempts = 0
    max_attempts = 5
    base_delay = 2  # seconds

    while retry_attempts < max_attempts:
        try:
            # your logic here
            do_something_risky()
            break  # success
        except SomeError as e:
            retry_attempts += 1
            delay = base_delay * (2 ** retry_attempts) + random.uniform(0, 1)
            kopf.logger.warning(f"Attempt {retry_attempts} failed: {e}. Retrying in {delay:.2f} seconds.")
            await asyncio.sleep(delay)
    else:
        raise kopf.TemporaryError("Max retries reached, still failing.", delay=60)