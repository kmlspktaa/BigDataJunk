# import modules
from pyspark.sql import SparkSession
from pyspark import SparkConf, SQLContext, HiveContext, SparkContext
import configparser

# set database configuration path here
auth_path = '/home/fieldemployee/bin/conf/db.ini'

# Define databases servers to use

db3 = "sqlserver"


config = configparser.ConfigParser()
config.read(auth_path)

config_f_format = configparser.ConfigParser()
config_f_format.read("/home/fieldemployee/Big_Data_Training/Spark/Python/sparkJobs/conf/file_formats.ini")

# sqlserver
sqlserver_user = config['sqlserver']['user']
sqlserver_password = config['sqlserver']['password']
sqlserver_driver_format = config['sqlserver']['driver_format']
sqlserver_url = config['sqlserver']['url']
sqlserver_connector_path = config['sqlserver']['driver_connector']

# Build spark session for each of the different RDBMS servers (mysql, sqlserver, postgresql)
# Mysql
spark = SparkSession.builder \
    .appName("Mysql_Spark_DF") \
    .master("local[*]") \
    .config("spark.jars", sqlserver_connector_path) \
    .getOrCreate()


path = "/user/input"


# Define file format here
#file_format = "csv"
file_format = config_f_format['file_format']['f_format']

def sqlserver_dataframe(user, password, table, driver_format, url, f_format):
    sqlserver_df = spark.read \
        .format(driver_format) \
        .option('url', url) \
        .option('user', user) \
        .option('password', password) \
        .option('dbtable', table) \
        .load()
    sqlserver_df.write.format(f_format).save(path + "/" + db3 + "/" + table)


path_tables = "/home/fieldemployee/PycharmProjects/sparkJobs/src/tables.txt"
table = ""
# Ingest Tables
with open(path_tables) as tables:
    for line in tables:
        # db, tb = line.partition("=")[::2]
        db, tb = str(line).split("=")

        # sqlserver
        if db.lower() == db3:
            table = tb.rstrip('\n')
            sqlserver_dataframe(sqlserver_user, sqlserver_password, table,
                                sqlserver_driver_format, sqlserver_url, file_format)

