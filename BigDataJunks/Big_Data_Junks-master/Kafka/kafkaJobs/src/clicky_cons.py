import sys

from kafka import KafkaConsumer
consumer = KafkaConsumer('admintome-pages', bootstrap_servers="localhost:9092",
                         auto_offset_reset='earliest')
try:
    for message in consumer:
        print(message.value)
except KeyboardInterrupt:
    sys.exit()