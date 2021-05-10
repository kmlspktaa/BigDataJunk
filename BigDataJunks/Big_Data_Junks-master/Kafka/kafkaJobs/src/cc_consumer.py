from kafka import KafkaConsumer
from json import loads
# import modules
from pyspark.sql import SparkSession
KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "testtopic"
# KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
spark = SparkSession.builder.appName("Json_DF").master("local[*]").getOrCreate()
if __name__ == "__main__":

    print("Kafka Consumer Application Started ... ")
    # auto_offset_reset='latest'
    # auto_offset_reset='earliest'
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME_CONS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    artist_name = []
    track_name = []
    popularity = []
    track_id = []
    year = []
    for track_results in consumer:
       # print("Key: ", track_results.key)
       y = track_results.key
       track_results = track_results.value
       # for i, t in enumerate(track_results['tracks']['items']):
       #     artist_name.append(t['artists'][0]['name'])
       #     track_name.append(t['name'])
       #     track_id.append(t['id'])
       #     popularity.append(t['popularity'])
       #     year.append(y)
       print("Message received: ", track_results)
       # json_df = spark.read.format("json").load(track_results)
       # json_df.show()
       #sc = spark.sparkContext
       #rdd = sc.parallelize(track_results)
       #print(rdd.collect())
       #df = spark.createDataFrame({'artist_name':artist_name,'track_name':track_name,'track_id':track_id,'popularity':popularity,'year':year})
       #df.show()
       # import pandas as pd
       # df_tracks = pd.DataFrame({'artist_name': artist_name, 'track_name': track_name, 'track_id': track_id, 'popularity': popularity, 'year': year})
       # print(df_tracks.shape)
       # print(df_tracks)
       # # Create a Spark DataFrame from a Pandas DataFrame using Arrow
       # Enable Arrow-based columnar data transfers


