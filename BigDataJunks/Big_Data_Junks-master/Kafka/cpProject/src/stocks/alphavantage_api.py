# Author: Abuchi Okeke
# Version: 0.0.5
# Date: 03/11/2020
# Description: #
from datetime import datetime
import kafka
import requests
import json
import time
#from json import dumps
KAFKA_TOPIC_NAME_CONS = "testtopic30"
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

await_termination = 20
while 100 > await_termination:
    response = stockdata_get({
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': SYM,
        'interval': '5min',
        'outputsize': 'full'})

    print("Response code: " + "%s" % response.status_code)

    dict = response.json()['Time Series (5min)']
    values = list(dict.values())
    dates = list(dict.keys())
    # r = [{"open":values[i]["1. open"], "high":values[i]["2. high"],"low":values[i]["3. low"],
    #       "close":values[i]["4. close"], "volume":values[i]["5. volume"],"date":dates[i]} for i in range(0,len(values))]

    # r = {"open": float(values[0]["1. open"]), "high": float(values[0]["2. high"]), "low": float(values[0]["3. low"]),
    # "close": float(values[0]["4. close"]), "volume": float(values[0]["5. volume"]), "date": dates[0]}

    # r = {"close": float(values[0]["4. close"]), "date": dates[0]}
    # print(type(r))
    # for i in range(1,len(values)):
    #    r["close"].append(float(values[i]["4. close"]))
    #    r["date"].append(dates[i])

    for i in range(0,len(values)):
        r = {"open": float(values[i]["1. open"]), "high": float(values[i]["2. high"]), "low": float(values[i]["3. low"]),
          "close": float(values[i]["4. close"]), "volume": float(values[i]["5. volume"]), "date": dates[i]}

        print("Sending response %s: " %i, r)
        # print(values)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, r)
        #kafka_producer_obj.flush()

        # kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, values.json())
        # kafka_producer_obj.flush()

        # def jprint(obj):
        #     # create a formatted string of the Python JSON object
        #     text = json.dumps(obj, sort_keys=True, indent=4)
        #     print(text)
        #
        # jprint(response.json())

        time.sleep(1)

    time.sleep(60)


