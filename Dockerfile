# Use a minimal Python base image
FROM python:latest

# Install dependencies
RUN pip install --no-cache-dir kopf kubernetes requests confluent-kafka

# Create working directory
WORKDIR /app

# Copy your operator code into the image
COPY empty_copy.py .

# Run the operator
CMD ["kopf", "run", "--standalone", "empty_copy.py"]

