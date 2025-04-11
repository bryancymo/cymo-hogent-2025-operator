# Use a minimal Python base image
FROM python:latest

# Install dependencies
RUN pip install --no-cache-dir kopf kubernetes

# Create working directory
WORKDIR /app

# Copy your operator code into the image
COPY operator.py .

# Run the operator
CMD ["kopf", "run", "--standalone", "OperatorRO.py"]
