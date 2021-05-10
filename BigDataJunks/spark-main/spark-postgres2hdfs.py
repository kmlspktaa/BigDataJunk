#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark import SparkConf,SQLContext,HiveContext,SparkContext


# In[2]:


spark = SparkSession.builder.appName("Posgres_Spark_DF").master("local[*]")                    .config("spark.jars","/home/morara/Downloads/spark-3.0.0-bin-hadoop2.7/jars/mysql-connector-java-8.0.17.jar").getOrCreate()


# In[3]:


sc = spark.sparkContext
sqlc = SQLContext(sc)


# In[4]:


print(sc)


# In[5]:


wal201_df = spark.read     .format("jdbc")     .option('url', 'jdbc:postgresql://localhost:5432/walstore2')     .option('user', 'morara')     .option('password', 'd3barl')     .option('dbtable', 'walst_201')     .load()


# In[6]:


wal202_df = spark.read     .format("jdbc")     .option('url', 'jdbc:postgresql://localhost:5432/walstore2')     .option('user', 'morara')     .option('password', 'd3barl')     .option('dbtable', 'walst_202')     .load()


# In[7]:


wal203_df = spark.read     .format("jdbc")     .option('url', 'jdbc:postgresql://localhost:5432/walstore2')     .option('user', 'morara')     .option('password', 'd3barl')     .option('dbtable', 'walst_203')     .load()


# In[11]:


wal201_df.write.format("csv").save("/user/input/walstore2/walst_201")


# In[12]:


wal202_df.write.format("csv").save("/user/input/walstore2/walst_202")


# In[13]:


wal203_df.write.format("csv").save("/user/input/walstore2/walst_203")


# In[ ]:




