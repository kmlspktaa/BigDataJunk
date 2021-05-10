# import modules
from pyspark.sql import SparkSession
from pyspark import SparkConf, SQLContext, HiveContext, SparkContext
import configparser

# set database configuration path here
auth_path = '/home/fieldemployee/bin/conf/db.ini'

# Define databases servers to use

db2 = "postgresql"

config = configparser.ConfigParser()
config.read(auth_path)

config_f_format = configparser.ConfigParser()
config_f_format.read("/home/fieldemployee/Big_Data_Training/Spark/Python/sparkJobs/conf/file_formats.ini")

# postgresql
postgresql_user = config['postgresql']['user']
postgresql_password = config['postgresql']['password']
postgresql_driver_format = config['postgresql']['driver_format']
postgresql_url = config['postgresql']['url']
postgresql_connector_path = config['postgresql']['driver_connector']

# Build spark session for each of the different RDBMS servers (mysql, sqlserver, postgresql)
# Mysql
spark = SparkSession.builder \
    .appName("Mysql_Spark_DF") \
    .master("local[*]") \
    .config("spark.jars", postgresql_connector_path) \
    .getOrCreate()

path = "/user/input"


# Define file format here
#file_format = "csv"
file_format = config_f_format['file_format']['f_format']

def postgresql_dataframe(user, password, table, driver_format, url, f_format):
    postgresql_df = spark.read \
        .format(driver_format) \
        .option('url', url) \
        .option('user', user) \
        .option('password', password) \
        .option('dbtable', table) \
        .load()
    postgresql_df.write.format(f_format).save(path + "/" + db2 + "/" + table)

path_tables = "/home/fieldemployee/PycharmProjects/sparkJobs/src/tables.txt"
table = ""
# Ingest Tables
with open(path_tables) as tables:
    for line in tables:
        # db, tb = line.partition("=")[::2]
        db, tb = str(line).split("=")
        # postgresql
        if db.lower() == db2:
            table = tb.rstrip('\n')
            postgresql_dataframe(postgresql_user, postgresql_password, table,
                                 postgresql_driver_format, postgresql_url, file_format)





