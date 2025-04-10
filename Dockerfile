FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY operatorJB.py .

CMD ["kopf", "run", "/app/operatorJB.py", "--standalone", "--verbose"]
