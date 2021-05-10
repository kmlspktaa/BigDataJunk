
"""
Spark structured streaming
11/07/2020
Author@ Abuchi Okeke
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField,StringType,IntegerType,FloatType,StructType)
from pyspark.sql import functions as func
# from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.types import DataType as dy
import numpy as np
import pandas as pd

from json import loads
import configparser

from pyspark.streaming.kafka import KafkaUtils

# kafkaStream = KafkaUtils.createStream(streamingContext, \
#      [ZK quorum], ["test_consumer_group"], [per-topic number of Kafka partitions to consume])


#set database configuration path here
auth_path = '/home/fieldemployee/bin/conf/db.ini'

config = configparser.ConfigParser()
config.read(auth_path)

sqlserver_user = config['sqlserver']['user']
sqlserver_password = config['sqlserver']['password']
sqlserver_driver_format = config['sqlserver']['driver_format']
#sqlserver_url = config['sqlserver']['url']
sqlserver_url = 'jdbc:sqlserver://localhost:1433;databaseName=stockdb'
sqlserver_connector_path = config['sqlserver']['driver_connector']


kafka_jar = '/usr/local/spark-2.4.7/jars/spark-sql-kafka-0-10_2.11-2.4.7.jar'
kafka_c = '/usr/local/spark-2.4.7/jars/kafka-clients-2.4.1.jar'


# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("Api_Spark_DF") \
    .master("local[*]") \
    .config("spark.jars", kafka_jar) \
    .config("spark.jars", kafka_c)\
    .enableHiveSupport() \
    .getOrCreate()

# Enable Arrow-based columnar data transfers
#spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", 5)
spark.sparkContext.setLogLevel("ERROR")

# Subscribe to 1 topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "testtopic30") \
  .option("startingOffsets", "latest")\
  .load()

# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
print("Printing Schema of orders_df: ")
df.printSchema()


df2 = df.selectExpr("CAST(value AS STRING)", "timestamp")

schema = StructType() \
         .add("open", StringType()) \
         .add("high", StringType()) \
         .add("low", StringType()) \
         .add("close", StringType()) \
         .add("volume", StringType())\
         .add('date', StringType())

df3 = df2.select(func.from_json(func.col("value"), schema)\
        .alias("stocks"), "timestamp")

df4 = df3.select("stocks.*", "timestamp")

#Aggregation
#convert days to seconds

days = lambda i: i * 86400

df5 = df4.withColumn('open', df4.open.cast('float'))\
         .withColumn('high', df4.high.cast('float'))\
         .withColumn('low', df4.low.cast('float'))\
         .withColumn('close', df4.close.cast('float'))\
         .withColumn('volume', df4.volume.cast('integer'))\
         .withColumn('date', df4.date.cast('timestamp'))\

df5.printSchema()

#windowSpec  = Window.partitionBy("department").orderBy("salary")
windowSpecAgg  = Window.partitionBy("date")

#df6 = df5.withColumn('rolling_average', func.avg("close").over(w))

# #from pyspark.sql.functions import col,avg,sum,min,max,row_number
# df6 = df5.withColumn("row",func.row_number().over(windowSpec)) \
#   .withColumn("avg", func.avg(func.col("close")).over(windowSpecAgg)) \
#   .withColumn("sum", sum(func.col("close")).over(windowSpecAgg)) \
#   .withColumn("min", min(func.col("close")).over(windowSpecAgg)) \
#   .withColumn("max", max(func.col("close")).over(windowSpecAgg)) \
#   .where(func.col("row")==1).select("open", "high","low", "close", "volume", "date","avg","sum","min","max") \


# # Simple aggregate - find total_order_amount by grouping country, city
# orders_df4 = orders_df3.groupBy("order_country_name", "order_city_name") \
#     .agg({'order_amount': 'sum'}) \
#     .select("order_country_name", "order_city_name", col("sum(order_amount)") \
#             .alias("total_order_amount"))

df6 = df5.withWatermark("timestamp", "5 seconds").groupBy("open", "high","low", "close", "volume", 'date', 'timestamp')\
    .agg({"close": 'avg'})
df7 = df6.select("open", "high","low", "close", "volume", "date", 'timestamp', func.col("avg(close)")) \
   .alias("rolling_average")

#df6.printSchema()
#inesWithSparkGDF = linesWithSparkDF.groupBy(col("id")).agg({"cycle": "max"})


# df2 = df.select(func.get_json_object(func.col("value").cast("string"), '$.open').alias('open'),
#                 func.get_json_object(func.col("value").cast("string"), '$.high').alias('high'),
#                 func.get_json_object(func.col("value").cast("string"), '$.low').alias('low'),
#                 func.get_json_object(func.col("value").cast("string"), '$.close').alias('close'),
#                 func.get_json_object(func.col("value").cast("string"), '$.volume').alias('volume'),
#                 func.get_json_object(func.col("value").cast("string"), '$.date').alias('date'))
#
#
#write to console
query2 = df7 \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "file:///home/fieldemployee/bin/kafka/checkpointing/") \
    .start()


#write to hive internal tables
def handle_hive(df, batch_id):
        df.write.saveAsTable(name='dsl.stocks_new', format='hive', mode='append')

query5 = df7.writeStream\
    .foreachBatch(handle_hive)\
    .option("checkpointLocation", "file:///home/fieldemployee/bin/kafka/hive/")\
    .start()

#query5.awaitTermination()
# def transform_with_pandas(df, batch_id):
#     # Convert the Spark DataFrame back to a pandas DataFrame using Arrow
#     df_pd = df.toPandas()
#     df_pd['SMA1'] = df_pd['close'].rolling(window=50).mean()
#     df_pd['SMA2'] = df_pd['close'].rolling(window=200).mean()
#     spark.createDataFrame(df_pd)
#
# query6 = df5.writeStream.foreachBatch(transform_with_pandas).start()

#write to sqlserver
def handle_sqlserver(df, batch_id):
     df.write \
    .format(sqlserver_driver_format) \
    .mode('append') \
    .option('url', sqlserver_url) \
    .option('user', sqlserver_user) \
    .option('password', sqlserver_password) \
    .option('dbtable', "stocks_new2_dev") \
    .save()

query6 = df7.writeStream\
    .foreachBatch(handle_sqlserver)\
    .option("checkpointLocation", "file:///home/fieldemployee/bin/kafka/sqlserver/") \
    .start()\


query6.awaitTermination()
# #write to hdfs & hive_external table
# query7 = df5.writeStream \
#   .format("csv")\
#   .outputMode("append")\
#   .option("checkpointLocation", "hdfs:///user/kafka/checkpointing/") \
#   .option("path", "/user/kafka/streams/")\
#   .start()
#
# query7.awaitTermination()

# #write to hdfs & hive_external table
# query7 = df5.writeStream \
#   .format("csv")\
#   .outputMode("append")\
#   .option("checkpointLocation", "file:///home/fieldemployee/bin/kafka/checkpointing/") \
#   .option("path", "file:///home/fieldemployee/bin/kafka/streams/")\
#   .start()
#
# query7.awaitTermination()

print("streaming recieved")

print("Stream Data Processing Application Completed.")





