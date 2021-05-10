#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark import SparkConf,SQLContext,HiveContext,SparkContext


# In[2]:


spark = SparkSession.builder.appName("mssql_Spark_DF").master("local[*]")                    .config("spark.jars","/home/morara/Downloads/spark-3.0.0-bin-hadoop2.7/jars/mssql-jdbc-8.4.1.jre8.jar").getOrCreate()


# In[3]:


sc = spark.sparkContext
sqlc = SQLContext(sc)


# In[4]:


print(sc)


# In[5]:


wal401_df = spark.read     .format("jdbc")     .option('url', "jdbc:sqlserver://localhost:1433;databaseName=walstore4")     .option('user', 'SA')     .option('password', 'C3v6g7@mn')     .option('dbtable', 'walst_401')     .load()


# In[6]:


wal402_df = spark.read     .format("jdbc")     .option('url', "jdbc:sqlserver://localhost:1433;databaseName=walstore4")     .option('user', "SA")     .option('password', "C3v6g7@mn")     .option('dbtable', 'walst_402')     .load()


# In[7]:


wal403_df = spark.read     .format("jdbc")     .option('url', "jdbc:sqlserver://localhost:1433;databaseName=walstore4")     .option('user', "SA")     .option('password', "C3v6g7@mn")     .option('dbtable', 'walst_403')     .load()


# In[8]:


wal401_df.write.format("csv").save("/user/input/walstore4/walst_401")


# In[9]:


wal402_df.write.format("csv").save("/user/input/walstore4/walst_402")


# In[10]:


wal403_df.write.format("csv").save("/user/input/walstore4/walst_403")


# In[ ]:




