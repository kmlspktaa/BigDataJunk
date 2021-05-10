# Author: Abuchi Okeke
# Version: 0.0.5
# Date: 28/10/2020
# Description: A pyspark program to create hive internal table from hive external table


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



# Query external tables
albums_int = spark.sql("SELECT * FROM raw.albums_external")
artist_int = spark.sql("SELECT * FROM raw.artist_external")
# auf1_int= spark.sql("SELECT * FROM dsl.auf1")
# auf2_int = spark.sql("SELECT * FROM dsl.auf2")
# auf3_int = spark.sql("SELECT * FROM dsl.auf3")
# auf4_int = spark.sql("SELECT * FROM dsl.auf4")
# auf5_int = spark.sql("SELECT * FROM dsl.auf5")
# auf6_int = spark.sql("SELECT * FROM dsl.auf6")
# auf7_int= spark.sql("SELECT * FROM dsl.auf7")
# auf8_int = spark.sql("SELECT * FROM dsl.auf8")
edges_int = spark.sql("SELECT * FROM raw.edges_external")
genre_int = spark.sql("SELECT * FROM raw.genre_external")
opensource_int = spark.sql("SELECT * FROM raw.opensource_external")
playlist_int = spark.sql("SELECT * FROM raw.playlist_external")
playlisttrack_int = spark.sql("SELECT * FROM raw.playlisttrack_external")
subgenre_int = spark.sql("SELECT * FROM raw.subgenre_external")
tracks_int = spark.sql("SELECT * FROM raw.tracks_external")
year_int = spark.sql("SELECT * FROM raw.year_external")
spotify_data_int = spark.sql("SELECT * FROM raw.spotify_music_data")

track_details = spark.sql("select\
    id,\
    artist_name,\
    track_name,\
    track_id,\
    popularity,\
    year from raw.spotify_music_data")

# DSL Layer
# Save tables to hive internal database
albums_int.write.format("orc").saveAsTable("dsl.albums_internal")
artist_int.write.format("orc").saveAsTable("dsl.artist_internal")
edges_int.write.format("orc").saveAsTable("dsl.edges_internal")
genre_int.write.format("orc").saveAsTable("dsl.genre_internal")
opensource_int.write.format("orc").saveAsTable("dsl.opensource_internal")
subgenre_int.write.format("orc").saveAsTable("dsl.subgenre_internal")
tracks_int.write.format("orc").saveAsTable("dsl.tracks_internal")
year_int.write.format("orc").saveAsTable("dsl.year_internal")
playlist_int.write.format("orc").saveAsTable("dsl.playlist_internal")
playlisttrack_int.write.format("orc").saveAsTable("dsl.playlisttrack_internal")
spotify_data_int.write.format("orc").saveAsTable("dsl.spotify_data_internal")
track_details.write.format("orc").saveAsTable("dsl.track_details")

# Query internal tables in DSL layer
pop_song = spark.sql("select * from dsl.track_details \
    order by popularity, year desc \
    limit 20")

top_song_2020_df = spark.sql("select * from dsl.track_details \
    where year = 2020")

# ASL layer
pop_song.write.format("orc").saveAsTable("asl.popularity_sorted")
top_song_2020_df.write.format("orc").saveAsTable("asl.top_song_2020")
