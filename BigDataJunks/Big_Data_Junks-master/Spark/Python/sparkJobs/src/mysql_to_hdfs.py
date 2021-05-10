# import modules
from pyspark.sql import SparkSession
from pyspark import SparkConf, SQLContext, HiveContext, SparkContext
import configparser

# set database configuration path here
auth_path = '/home/fieldemployee/bin/conf/db.ini'

# Define databases servers to use

db1 = "mysql"

config = configparser.ConfigParser()
config.read(auth_path)

config_f_format = configparser.ConfigParser()
config_f_format.read("/home/fieldemployee/Big_Data_Training/Spark/Python/sparkJobs/conf/file_formats.ini")


# Do not edit
# mysql
mysql_user = config['mysql']['user']
mysql_password = config['mysql']['password']
mysql_driver_format = config['mysql']['driver_format']
mysql_url = config['mysql']['url']
mysql_connector_path = config['mysql']['driver_connector']

# Build spark session for each of the different RDBMS servers (mysql, sqlserver, postgresql)
# Mysql
spark = SparkSession.builder \
    .appName("Mysql_Spark_DF") \
    .master("local[*]") \
    .config("spark.jars", mysql_connector_path) \
    .getOrCreate()

path = "/user/input"


# Define file format here
#file_format = "csv"
file_format = config_f_format['file_format']['f_format']


# Dataframe functions for each of the databases
def mysql_dataframe(user, password, table, driver_format, url, f_format):
    mysql_df = spark.read \
        .format(driver_format) \
        .option('url', url) \
        .option('user', user) \
        .option('password', password) \
        .option('dbtable', table) \
        .load()
    mysql_df.write.format(f_format).save(path + "/" + db1 + "/" + table)


path_tables = "/home/fieldemployee/PycharmProjects/sparkJobs/src/tables.txt"
table = ""
# Ingest Tables
with open(path_tables) as tables:
    for line in tables:
        # db, tb = line.partition("=")[::2]
        db, tb = str(line).split("=")
        # mysql
        if db.lower() == db1:
            table = tb.rstrip('\n')
            mysql_dataframe(mysql_user, mysql_password, table,
                            mysql_driver_format, mysql_url, file_format)
