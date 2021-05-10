# Author: Abuchi Okeke
# Version: 0.0.5
# Date: 26/10/2020
# Description: A pyspark program for ingesting tables from Mysql, postgresql, sqlserver databases and csv files to hdfs

# import modules
from pyspark.sql import SparkSession
from pyspark import SparkConf, SQLContext, HiveContext, SparkContext
import configparser

# set database configuration path here
auth_path = '/home/fieldemployee/bin/conf/db.ini'

# Define databases servers to use
db1 = "mysql"
db2 = "postgresql"
db3 = "sqlserver"

# TODO 11/3/2020....
db4 = "oracle"

# Get passwords
# mysql
# mysql_f = open('/home/fieldemployee/bin/passwords/mysql.password')
# mysql_password = mysql_f.readline()
# mysql_f.close()
# # postgresql
# postgresql_f = open('/home/fieldemployee/bin/passwords/postgres_local.password')
# postgresql_password = postgresql_f.readline()
# postgresql_f.close()

# Get authentication details for each of the databases server
config = configparser.ConfigParser()
config.read(auth_path)

config_f_format = configparser.ConfigParser()
config_f_format.read("/home/fieldemployee/Big_Data_Training/Spark/Python/sparkJobs/conf/file_formats.ini")

"""
To configure you databases server
1. Goto the conf folder
2. Open the db.ini
3. Set all your parameter for each of the databases
"""

# Do not edit
# mysql
mysql_user = config['mysql']['user']
mysql_password = config['mysql']['password']
mysql_driver_format = config['mysql']['driver_format']
mysql_url = config['mysql']['url']
mysql_connector_path = config['mysql']['driver_connector']

# postgresql
postgresql_user = config['postgresql']['user']
postgresql_password = config['postgresql']['password']
postgresql_driver_format = config['postgresql']['driver_format']
postgresql_url = config['postgresql']['url']
postgresql_connector_path = config['postgresql']['driver_connector']

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
    .config("spark.jars", mysql_connector_path) \
    .config("spark.jars", postgresql_connector_path) \
    .config("spark.jars", sqlserver_connector_path) \
    .getOrCreate()

# # Postgresql
# spark_postgresql = SparkSession.builder \
#     .appName("Postgresql_Spark_DF") \
#     .master("local[*]") \
#     .config("spark.jars", postgresql_connector_path) \
#     .getOrCreate()
#
# # sqlServer
# spark_sqlserver = SparkSession.builder \
#     .appName("SqlServer_Spark_DF") \
#     .master("local[*]") \
#     .config("spark.jars", sqlserver_connector_path) \
#     .getOrCreate()

# Create RDD for the different RDBMS using spark context
sc = spark.sparkContext
# sc_postgresql = spark_postgresql.sparkContext
# sc_sqlserver = spark_sqlserver.sparkContext

# Sql context connector for each of the servers
# sqlc_mysql = SQLContext(sc_mysql)
# sqlc_postgresql = SQLContext(sc_postgresql)
# sqlc_sqlserver = SQLContext(sc_sqlserver)

# Hadoop Path
# path = "hdfs://localhost:9000/user/input/"
path = "/user/input"

# local file path
# path = "file:///home/fieldemployee/spark"

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


def postgresql_dataframe(user, password, table, driver_format, url, f_format):
    postgresql_df = spark.read \
        .format(driver_format) \
        .option('url', url) \
        .option('user', user) \
        .option('password', password) \
        .option('dbtable', table) \
        .load()
    postgresql_df.write.format(f_format).save(path + "/" + db2 + "/" + table)


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
        # mysql
        if db.lower() == db1:
            table = tb.rstrip('\n')
            mysql_dataframe(mysql_user, mysql_password, table,
                            mysql_driver_format, mysql_url, file_format)

        # postgresql
        if db.lower() == db2:
            table = tb.rstrip('\n')
            postgresql_dataframe(postgresql_user, postgresql_password, table,
                                 postgresql_driver_format, postgresql_url, file_format)

        # sqlserver
        if db.lower() == db3:
            table = tb.rstrip('\n')
            sqlserver_dataframe(sqlserver_user, sqlserver_password, table,
                                sqlserver_driver_format, sqlserver_url, file_format)

# Get CSV files
# set datasets path
dataset_path = "file:///home/fieldemployee/bin/datasets/"
spark_csv = SparkSession.builder.appName("CSV_DF").master("local[*]").getOrCreate()
csv_opensource_df = spark_csv.read \
    .format("csv").option("header", "true") \
    .option("inferSchema", "true") \
    .load(dataset_path + "opensource.csv")
csv_opensource_df.write.format(file_format).save(path + "/csv/opensource")

csv_spotify_df = spark_csv.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .load(dataset_path + "spotify.csv")
csv_spotify_df.write.format(file_format).save(path + "/csv/spotify_api")

# df.coalesce(1).write.format('com.databricks.spark.csv').save('path+my.csv',header = 'true')
