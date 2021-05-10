# Author: Abuchi Okeke
# Version: 0.0.5
# Date: 28/10/2020
# Description: A pyspark program for ingesting tables from hive internal tables to postgresql database server


from pyspark.sql import SparkSession
from datetime import datetime
import configparser

# set database configuration path here
auth_path = '/home/fieldemployee/bin/conf/db.ini'

# Get current date time
date_time = (datetime.now().strftime('%Y%m%d%H%M%S'))

config = configparser.ConfigParser()
config.read(auth_path)

# postgresql
postgresql_user = config['postgresql']['user']
postgresql_password = config['postgresql']['password']
postgresql_driver_format = config['postgresql']['driver_format']
# postgresql_url = config['postgresql']['url']
postgresql_url = "jdbc:postgresql://localhost:5432/spotifydb"
postgresql_connector_path = config['postgresql']['driver_connector']

spark = SparkSession \
    .builder \
    .appName("Hive_Spark_DF") \
    .config("spark.jars", postgresql_connector_path) \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

# csv_df.write.format("orc").saveAsTable("test.csv_df")
albums_tb = spark.sql("SELECT * FROM dsl.albums_internal")
artist_tb = spark.sql("SELECT * FROM dsl.artist_internal")
# auf1_tb = spark.sql("SELECT * FROM dsl.auf1")
# auf2_tb = spark.sql("SELECT * FROM dsl.auf2")
# auf3_tb = spark.sql("SELECT * FROM dsl.auf3")
# auf4_tb = spark.sql("SELECT * FROM dsl.auf4")
# auf5_tb = spark.sql("SELECT * FROM dsl.auf5")
# auf6_tb = spark.sql("SELECT * FROM dsl.auf6")
# auf7_tb = spark.sql("SELECT * FROM dsl.auf7")
# auf8_tb = spark.sql("SELECT * FROM dsl.auf8")
edges_tb = spark.sql("SELECT * FROM dsl.edges_internal")
genre_tb = spark.sql("SELECT * FROM dsl.genre_internal")
opensource_tb = spark.sql("SELECT * FROM dsl.opensource_internal")
playlist_tb = spark.sql("SELECT * FROM dsl.playlist_internal")
playlisttrack_tb = spark.sql("SELECT * FROM dsl.playlisttrack_internal")
track_details_tb = spark.sql("SELECT * FROM dsl.track_details")
subgenre_tb = spark.sql("SELECT * FROM dsl.subgenre_internal")
tracks_tb = spark.sql("SELECT * FROM dsl.tracks_internal")
year_tb = spark.sql("SELECT * FROM dsl.year_internal")
spotify_tb = spark.sql("SELECT * FROM dsl.spotify_data_internal")


def postgresql_save_to_rdbms(tb_df, table, user, password, driver_format, url):
    tb_df.write \
        .format(driver_format) \
        .option('url', url) \
        .option('user', user) \
        .option('password', password) \
        .option('dbtable', table + str(date_time)) \
        .save()

# Save tables to postgres rdbms
postgresql_save_to_rdbms(albums_tb, "albums", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(genre_tb, "genre", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(subgenre_tb, "sungenre", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(year_tb, "year" ,postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(tracks_tb, "tracks", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(edges_tb, "edges", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(artist_tb,"artist", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
# postgresql_save_to_rdbms(auf1_tb, "auf1", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
# postgresql_save_to_rdbms(auf2_tb, "auf2", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
# postgresql_save_to_rdbms(auf3_tb, "auf3", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
# postgresql_save_to_rdbms(auf4_tb, "auf4", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
# postgresql_save_to_rdbms(auf5_tb, "auf5", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
# postgresql_save_to_rdbms(auf6_tb, "auf6", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
# postgresql_save_to_rdbms(auf7_tb, "auf7", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
# postgresql_save_to_rdbms(auf8_tb, "auf8", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(playlist_tb, "palylist", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(playlisttrack_tb, "playlisttrack", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(opensource_tb, "opensource", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(track_details_tb, "track_details", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
postgresql_save_to_rdbms(spotify_tb, "spotify_data", postgresql_user, postgresql_password, postgresql_driver_format, postgresql_url)
