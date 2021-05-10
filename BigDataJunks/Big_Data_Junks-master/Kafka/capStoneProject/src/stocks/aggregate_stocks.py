"""

Author: @Abuchi Okeke
Date: 11/07/2020

"""

import pyspark
# from pyspark.sql.functions import approx_count_distinct,collect_list
# from pyspark.sql.functions import collect_set, sum,avg,max,countDistinct,count
# from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
# from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
# from pyspark.sql.functions import variance,var_samp,  var_pop
from pyspark.sql import functions

from pyspark.sql import functions as func

# noinspection PyUnresolvedReferences
# from pyspark.sql.functions import (
#     col as pyspark_col, count as pyspark_count, expr as pyspark_expr,
#     floor as pyspark_floor, log1p as pyspark_log1p, upper as pyspark_upper,
# )

from pyspark.sql import SparkSession
import configparser
from datetime import datetime


# set database configuration path here
auth_path = '/home/fieldemployee/bin/conf/db.ini'

db1 = 'mysql'

# Get current date time
date_time = (datetime.now().strftime('%Y%m%d%H%M%S'))
config = configparser.ConfigParser()
config.read(auth_path)

# get auth details for mysql
mysql_user = config['mysql']['user']
mysql_password = config['mysql']['password']
mysql_driver_format = config['mysql']['driver_format']
# mysql_url = config['mysql']['url']
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


# function to save to RDBMS
def save_to_rdbms(tb_df, table, user, password, driver_format, url):
    tb_df.write \
        .format(driver_format) \
        .mode('overwrite') \
        .option('url', url) \
        .option('user', user) \
        .option('password', password) \
        .option('dbtable', table) \
        .save()


# Dataframe functions for each of the databases
def mysql_dataframe(user, password, table, driver_format, url, f_format):
    mysql_df = spark.read \
        .format(driver_format) \
        .option('url', url) \
        .option('user', user) \
        .option('password', password) \
        .option('dbtable', table) \
        .load()
    return mysql_df


path_tables = '/home/fieldemployee/Big_Data_Training/Kafka/kafkaJobs/src/tables/tables.txt'
file_format = "orc"
table1 = "stocks"


# spark sum aggregation

def stocks_hive_tb():
    return spark.sql("""SELECT SUM(close), SUM(SMA1), SUM(SMA2) FROM dsl.stocks""")


stocks_mysql_tb = mysql_dataframe(mysql_user, mysql_password, table1, mysql_driver_format, mysql_url, file_format)

# print("hive table:")
sum_close = stocks_hive_tb().collect()[0]
print(sum_close)
#


"""
There are several spark functions that have wrappers generated at runtime by adding to the globals dict, 
then adding those to __all__. As pointed out by @vincent-claes referencing the functions using the 
function path (as F or as something else, I prefer something more descriptive) can make it so the imports 
don't show an error in PyCharm. However, as @nexaspx alluded to in a comment on that answer, that shifts the 
warning to the usage line(s). As mentioned by @thomas pyspark-stubs can be installed to improve the situation.
"""


def sum_plotting_data():
    return stocks_mysql_tb.select(func.sum("close"), func.sum('SMA1'), func.sum('SMA2'))

print("mysql_table:")
sum_mysql_close = sum_plotting_data().collect()[0]
print(sum_mysql_close)
