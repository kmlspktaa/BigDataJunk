# import modules
from pyspark.sql import SparkSession
from pyspark import SparkConf, SQLContext, HiveContext, SparkContext
import configparser



config_f_format = configparser.ConfigParser()
config_f_format.read("/home/fieldemployee/Big_Data_Training/Spark/Python/sparkJobs/conf/file_formats.ini")

path = "/user/input"


# Define file format here
#file_format = "csv"
file_format = config_f_format['file_format']['f_format']

# Get CSV files
# set datasets path
dataset_path = "file:///home/fieldemployee/bin/datasets/"
spark_csv = SparkSession.builder.appName("CSV_DF").master("local[*]").getOrCreate()
csv_opensource_df = spark_csv.read \
    .format("csv").option("header", "false") \
    .option("inferSchema", "true") \
    .load(dataset_path + "opensource.csv")
csv_opensource_df.write.format(file_format).save(path + "/csv/opensource")

csv_spotify_df = spark_csv.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .load(dataset_path + "spotify.csv")
csv_spotify_df.write.format(file_format).save(path + "/csv/spotify_api")

# df.coalesce(1).write.format('com.databricks.spark.csv').save('path+my.csv',header = 'true')

