from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

cid = 'fda982ead35c41c2b8b3e2763e7a0304'
secret = '7de8b0a3d1ca447f8db9d509ce8e5f1d'
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# timeit library to measure the time needed to run this code
import timeit

start = timeit.default_timer()

KAFKA_TOPIC_NAME_CONS = "testtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    # transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]

    # message = None
    # for i in range(100):
    #     i = i + 1
    #     message = {}
    #     print("Sending message to Kafka topic: " + str(i))
    #     event_datetime = datetime.now()
    #
    #     message["transaction_id"] = str(i)
    #     message["transaction_card_type"] = random.choice(transaction_card_type_list)
    #     message["transaction_amount"] = round(random.uniform(5.5,555.5), 2)
    #     message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
    #
    #     print("Message to be sent: ", message)
    #     kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
    #     time.sleep(1)
    y_start = 1920  # Year Start
    y_end = 2020  # Year End
    for y in range(y_start, y_end, 1):
        for i in range(0, 2000, 50):
            name = 'Define artist name'  # Popularity by artist name

            track_results = sp.search('year:' + str(y), type='track', limit=50, offset=i)
            kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, track_results)

        stop = timeit.default_timer()
        print('Time to run this code (in seconds):', stop - start)
