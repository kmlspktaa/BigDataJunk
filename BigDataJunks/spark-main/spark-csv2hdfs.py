#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField,StringType,IntegerType,FloatType,StructType)


# In[2]:


os.environ["SPARK_HOME"] = "/home/morara/Downloads/spark-3.0.0-bin-hadoop2.7"
spark = SparkSession        .builder        .appName("Hive_Spark_DF")        .master("local[*]")        .config("spark.sql.warehouse.dir","/user/hive/warehouse")        .enableHiveSupport()        .getOrCreate()


# In[3]:


data_schema = [StructField('Date',StringType(),True),
               StructField('Store',IntegerType(),True),
               StructField('Dept',IntegerType(),True),
               StructField('Temperature',FloatType(),True),
               StructField('Fuel_Price',FloatType(),True),
               StructField('CPI',FloatType(),True),
               StructField('Unemployment',FloatType(),True),
               StructField('IsHoliday',StringType(),True),
               StructField('Weekly_Sales',FloatType(),True)]


# In[4]:


struc = StructType(fields=data_schema)


# In[5]:


wal301_df = spark.read.csv('file:///home/morara/data/walst_301.csv',schema=struc,header=True)


# In[6]:


wal302_df = spark.read.csv('file:///home/morara/data/walst_302.csv',schema=struc,header=True)


# In[7]:


wal303_df = spark.read.csv('file:///home/morara/data/walst_303.csv',schema=struc,header=True)


# In[8]:


#wal301_df.show()


# In[9]:


wal301_df.write.format("csv").save("/user/input/walstore3/walst_301")


# In[10]:


wal302_df.write.format("csv").save("/user/input/walstore3/walst_302")


# In[11]:


wal303_df.write.format("csv").save("/user/input/walstore3/walst_303")

