FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY basic-op.py .

CMD ["kopf", "run", "/app/basic-op.py", "--verbose"]
