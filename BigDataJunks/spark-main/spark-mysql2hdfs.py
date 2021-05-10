#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark import SparkConf,SQLContext,HiveContext,SparkContext


# In[2]:


spark = SparkSession.builder.appName("Mysql_Spark_DF").master("local[*]")                    .config("spark.jars","/home/morara/Downloads/spark-3.0.0-bin-hadoop2.7/jars/mysql-connector-java-8.0.17.jar").getOrCreate()


# In[3]:


sc = spark.sparkContext
sqlc = SQLContext(sc)


# In[4]:


print(sc)


# In[5]:


wal101_df = spark.read     .format("jdbc")     .option('url', 'jdbc:mysql://localhost:3306/walstore1')     .option('user', 'morara')     .option('password', 'd3barl')     .option('dbtable', 'walst_101')     .load()


# In[6]:


wal102_df = spark.read     .format("jdbc")     .option('url', 'jdbc:mysql://localhost:3306/walstore1')     .option('user', 'morara')     .option('password', 'd3barl')     .option('dbtable', 'walst_102')     .load()


# In[7]:


wal103_df = spark.read     .format("jdbc")     .option('url', 'jdbc:mysql://localhost:3306/walstore1')     .option('user', 'morara')     .option('password', 'd3barl')     .option('dbtable', 'walst_103')     .load()


# In[8]:


wal101_df.write.format("csv").save("/user/input/walstore1/walst_101")


# In[9]:


wal102_df.write.format("csv").save("/user/input/walstore1/walst_102")


# In[10]:


wal103_df.write.format("csv").save("/user/input/walstore1/walst_103")

