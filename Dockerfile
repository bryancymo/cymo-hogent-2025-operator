FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY operator.py .

CMD ["kopf", "run", "/app/operator2.py", "--verbose"]

CMD ["kopf", "run", "/app/operator-crd.py", "--verbose"]
