# Author: Abuchi Okeke
# Version: 0.0.5
# Date: 03/11/2020
# Description: #

import kafka
import requests
import json
#from json import dumps
KAFKA_TOPIC_NAME_CONS = "testtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

print("Kafka Producer Application Started ... ")

kafka_producer_obj = kafka.KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: json.dumps(x, sort_keys=True, indent=4).encode('utf-8'))

#Alphavantage
API_KEY = "04L9F1LKW1BKVM4S"
SYM = "IBM"
#response = requests.get('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=04L9F1LKW1BKVM4S')

def stockdata_get(payload):

    url = 'https://www.alphavantage.co/query?'

    # Add API key and format to the payload
    payload['apikey'] = API_KEY
    payload['datatype'] = 'json'

    response = requests.get(url, params=payload)
    return response

response = stockdata_get({
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': SYM,
    'interval': '5min',
    'outputsize': 'full'
})

print("Response code: " + "%s" % response.status_code)

kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, response.json())



def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

jprint(response.json())