# Use a Python base image (3.10 is fine)
FROM python:3.10

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt first, install dependencies
COPY requirements.txt .

# Install dependencies in a single layer to take advantage of Docker caching
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python operator scripts into the container
COPY operator2.py .
COPY operatorRO.py .

# Default entrypoint: use an environment variable to decide which operator to run
CMD ["sh", "-c", "kopf run /app/${OPERATOR_SCRIPT:-operator2.py} --standalone --verbose"]
