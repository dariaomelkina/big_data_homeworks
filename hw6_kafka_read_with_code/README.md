## HW 6: Kafka read with code

Create Kafka installation:
```
bash run-cluster.sh
```

Installation result:
![Installation result](images/inst.png)

Write tweets from file into Kafka (path to csv file is hardcoded in [write_to_kafka.py](write_to_kafka.py)):
```
bash write_to_kafka.sh
```
Script run results:
![Script run results](images/script.png)

Reading data using console client (running in separate terminal during writing process):
```
bash console_read.sh
```
Reading result:
![Reading result](images/read.png)


To shutdown Kafka cluster:
```
bash shutdown-cluster.sh
```