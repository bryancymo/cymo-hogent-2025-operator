FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY operator.py .

CMD ["kopf", "run", "/app/operator.py", "--verbose"]
