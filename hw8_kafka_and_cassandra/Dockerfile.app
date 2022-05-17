FROM python:3.9-slim

RUN apt-get update

RUN pip install --upgrade pip
RUN pip install cassandra-driver
RUN pip install flask

WORKDIR /python-docker

COPY rest_api/app.py .
COPY cassandra_part/CassandraClient.py .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=8080"]