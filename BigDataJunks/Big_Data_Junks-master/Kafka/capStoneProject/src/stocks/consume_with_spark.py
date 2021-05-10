from pyspark.sql import SparkSession

import configparser


# set database configuration path here
auth_path = '/home/fieldemployee/bin/conf/db.ini'

config = configparser.ConfigParser()
config.read(auth_path)

# get auth details for mysql
mysql_user = config['mysql']['user']
mysql_password = config['mysql']['password']
mysql_driver_format = config['mysql']['driver_format']
#mysql_url = config['mysql']['url']
mysql_url = 'jdbc:mysql://localhost:3306/stockdb'
mysql_connector_path = config['mysql']['driver_connector']


# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("Api_Spark_DF") \
    .master("local[*]") \
    .config("spark.jars", mysql_connector_path) \
    .enableHiveSupport() \
    .getOrCreate()

# Subscribe to 1 topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "testtopic") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df.show(3)