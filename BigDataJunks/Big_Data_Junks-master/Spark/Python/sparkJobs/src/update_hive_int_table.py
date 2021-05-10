# Author: Abuchi Okeke
# Version: 0.0.5
# Date: 28/10/2020
# Description: A pyspark program to overwrite hive internal tables


from pyspark.sql import SparkSession
from datetime import datetime

# Get current date time
date_time = (datetime.now().strftime('%Y%m%d%H%M%S'))

# Build spark session
spark = SparkSession \
    .builder \
    .appName("Hive_Spark_DF") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

# Query external tables to overwrite and update the tables
spark.sql("INSERT OVERWRITE TABLE dsl.albums_internal SELECT * FROM raw.albums_external")
spark.sql("INSERT OVERWRITE TABLE dsl.artist_internal SELECT * FROM raw.artist_external")
spark.sql("INSERT OVERWRITE TABLE dsl.edges_internal SELECT * FROM raw.edges_external")
spark.sql("INSERT OVERWRITE TABLE dsl.genre_internal SELECT * FROM raw.genre_external")
spark.sql("INSERT OVERWRITE TABLE dsl.opensource_internal\
                            SELECT * FROM raw.opensource_external")
spark.sql("INSERT OVERWRITE TABLE dsl.playlist_internal SELECT * FROM raw.playlist_external")
spark.sql("INSERT OVERWRITE TABLE dsl.playlisttrack_internal\
                                SELECT * FROM raw.playlisttrack_external")
spark.sql("INSERT OVERWRITE TABLE dsl.subgenre_internal SELECT * FROM raw.subgenre_external")
spark.sql("INSERT OVERWRITE TABLE dsl.tracks_internal SELECT * FROM raw.tracks_external")
spark.sql("INSERT OVERWRITE TABLE dsl.year_internal SELECT * FROM raw.year_external")
spark.sql("INSERT OVERWRITE TABLE dsl.spotify_data_internal SELECT * FROM raw.spotify_music_data")

spark.sql("INSERT OVERWRITE TABLE dsl.track_details select\
    id,\
    artist_name,\
    track_name,\
    track_id,\
    popularity,\
    year from raw.spotify_music_data")

# Query internal tables overwite tables in ASL layer
spark.sql("INSERT OVERWRITE TABLE asl.popularity_sorted select * from dsl.track_details \
    order by popularity, year desc \
    limit 20")

spark.sql("INSERT OVERWRITE TABLE asl.top_song_2020 select * from dsl.track_details \
    where year = 2020")


