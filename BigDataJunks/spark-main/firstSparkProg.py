from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("myFirstprog")\
        .master("local[*]")\
        .getOrCreate()

l = [10,20,30,40,50]

sc = spark.sparkContext

rdd = sc.parallelize(l)

print(rdd.collect())