FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY operator.py .

<<<<<<< HEAD
CMD ["kopf", "run", "/app/operator.py", "--verbose"]
=======
CMD ["kopf", "run", "/app/operator2.py", "--standalone", "--verbose"]
>>>>>>> 8be390c3f401bad2e1c9876d649b7521c483027c
