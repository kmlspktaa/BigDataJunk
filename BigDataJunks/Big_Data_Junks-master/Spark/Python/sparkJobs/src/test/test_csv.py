from pyspark.sql import SparkSession

path = "/user/input"
dataset_path = "file:///home/fieldemployee/bin/datasets/"
spark_csv = SparkSession.builder.appName("CSV_DF").master("local[*]").getOrCreate()
# csv_opensource_df = spark_csv.read \
#     .format("csv").option("header", "false") \
#     .option("inferSchema", "true") \
#     .load(dataset_path + "opensource.csv")
# csv_opensource_df.write.format("avro").save(path + "/csv/opensource")

csv_spotify_df = spark_csv.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(dataset_path + "spotify.csv")
csv_spotify_df.write.format("avro").save(path + "/csv/spotify_api")
# csv_opensource_df.show(10)
# csv_spotify_df.show(10)
