FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY operator2.py .

CMD ["kopf", "run", "/app/operator2.py", "--standalone", "--verbose"]