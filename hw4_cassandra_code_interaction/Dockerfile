FROM python:3.9-slim

RUN apt-get update

RUN pip install --upgrade pip

RUN pip install cassandra-driver

COPY py_scripts/read_from_cassandra.py /opt/app/
COPY py_scripts/CassandraClient.py /opt/app/

ENTRYPOINT ["python", "/opt/app/read_from_cassandra.py"]