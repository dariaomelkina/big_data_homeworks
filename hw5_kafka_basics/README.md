# HW 5: Kafka basics

To create Kafka cluster:
```
bash run-cluster.sh
```
Installation result:
![Installation result](images/inst.png)

Create topic test-topic:
```
bash create_topic.sh
```
Topic creation check:
```
bash check_topic.sh
```
![Topic creation check](images/topic.png)

Example of writing messages:
![Example of writing messages](images/write.png)

Example of reading messages:
![Example of reading messages](images/read.png)


To shutdown Kafka cluster:
```
bash shutdown-cluster.sh
```