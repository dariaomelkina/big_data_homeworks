FROM python:3.9-slim

RUN apt-get update

RUN pip install --upgrade pip
RUN pip install cassandra-driver
RUN pip install pandas
RUN pip install flask

WORKDIR /python-docker

COPY py_scripts/app.py .
COPY py_scripts/CassandraClient.py .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=8080"]