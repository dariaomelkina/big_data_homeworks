import json
import csv

from kafka import KafkaConsumer

if __name__ == "__main__":
    consumer = KafkaConsumer('tweets',
                             bootstrap_servers='kafka-server:9092',
                             value_deserializer=lambda m: json.loads(m.decode('ascii')))

    file_label = ""
    messages_to_save = []
    header = ['author_id', 'created_at', 'text']

    for message in consumer:
        new_file_label = f"tweets_{message.value['created_at']}.csv"

        if new_file_label != file_label:
            # Writing previous data into file
            with open('results/'+new_file_label, 'w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(messages_to_save)

            file_label = new_file_label
            messages_to_save = []

        messages_to_save.append([message.value['author_id'], message.value['created_at'], message.value['text']])


