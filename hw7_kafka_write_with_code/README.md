## HW 7: Kafka write with code

Create Kafka installation:
```
bash run-cluster.sh
```

Installation result:
![Installation result](images/inst.png)

Write tweets from file into Kafka (path to csv file is hardcoded in [write_to_kafka.py](write_to_kafka.py)):
```
bash write/write_to_kafka.sh
```
Script run results:
![Script run results](images/write_docker.png)

Reading data (in a second terminal instance):
```
bash read/read_from_kafka.sh
```
Script run results:
![Script run results](images/read_docker.png)

Reading results:
![Reading result](images/read_docker.png)

Files with example results are in [results/](results) directory. In order to write them directly into host, 
I had to pass absolute path to results directory in [read/read_from_kafka.sh](read/read_from_kafka.sh).

File screenshot example:
![File screenshot example](images/file.png)

To shutdown Kafka cluster:
```
bash shutdown-cluster.sh
```