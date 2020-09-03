#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install -q findspark


# In[2]:


import findspark
findspark.init('/home/bigdata/Documents/spark-3.0.0')


# In[3]:


import pandas as pd
import numpy as np
from pyspark.sql.session import SparkSession
import matplotlib.pyplot as plt
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import DataFrameReader
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from math import sqrt
import sys
import os
from pyspark.sql.types import IntegerType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, column, expr, when, lit
from pyspark.sql.functions import mean, min, max, sum, round, count, datediff, to_date
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F 
from pyspark.sql.functions import *


# In[4]:


spark = SparkSession.builder.appName('DLG').getOrCreate()


# In[5]:


data = spark.read.format("csv").option("header","True").load("/home/bigdata/Data Engineer Test/weather.20160301.csv")


# In[6]:


data.show()


# In[7]:


#read second weather dataset
data2 = spark.read.format("csv").option("header","True").load("/home/bigdata/Data Engineer Test/weather.20160201.csv")


# In[8]:


data2.show()


# In[9]:


data.printSchema()


# In[10]:


data.count()


# In[11]:


#show schema of the dataset
data2.printSchema()


# In[12]:


#count
data2.count()


# In[13]:


# merging two wether data together with no effect on all dataset
data_df = data.unionAll(data2)


# In[14]:


#count
data_df.count()


# In[15]:


#convert and save merged csv file to parquet file format
#data_df.write.parquet("/home/bigdata/parquet/dataset.parquet")


# In[16]:


#read dataset.parquet file
df =spark.read.format("parquet").option("header","True").load("/home/bigdata/parquet/dataset.parquet")


# In[17]:


df.show()


# In[18]:


#print schema
df.printSchema()


# In[19]:


#converted ObservationTime from string to Float
df= df.withColumn("ObservationTime", df["ObservationTime"].cast(FloatType()))


# In[20]:


from pyspark.sql.functions import unix_timestamp, from_unixtime
import numpy as np
import datetime
from pyspark.sql.functions import year, month, dayofmonth


# In[21]:


#removing T from ObservationDate
df = df.withColumn('ObservationDate', regexp_replace(col('ObservationDate'), "T", " "))


# In[22]:


#day (yyyy-mm-dd HH:mm:ss)
df.select("ObservationDate").show()


# In[23]:


#WindDirection converted from string to float
df= df.withColumn("WindDirection", df["WindDirection"].cast(FloatType()))


# In[24]:


#WindSpeed converted from string to integer
df= df.withColumn("WindSpeed", df["WindSpeed"].cast(IntegerType()))


# In[25]:


#visibilty converted from from to integer
df= df.withColumn("Visibility", df["Visibility"].cast(IntegerType()))


# In[26]:


#ScreenTemperature converted from string to float
df= df.withColumn("ScreenTemperature", df["ScreenTemperature"].cast(FloatType()))


# In[27]:


#Pressure converted from string to integer
df= df.withColumn("Pressure", df["Pressure"].cast(IntegerType()))


# In[28]:


#Latitude converted from string to Float
df= df.withColumn("Latitude", df["Latitude"].cast(FloatType()))


# In[29]:


#Longitude converted from string to Float
df= df.withColumn("Longitude", df["Longitude"].cast(FloatType()))


# In[30]:


#print schema after converting datatype
df.printSchema()


# In[31]:


#fill missing value integer with zero --no effect on result
df =df.na.fill(0)


# In[32]:


#show df dataset
df.show()


# In[33]:


#create a temporary view using DataFrame
df.createOrReplaceTempView("weather")


# In[34]:


#order by Descending ScreenTemperature to observe hottest day and region
maxtemp_df =spark.sql("select Region , ObservationDate, ScreenTemperature from weather order by ScreenTemperature DESC")


# In[35]:


#show hottest Region, Day and Temperature
maxtemp_df.show()


# In[36]:


#hottest day(highest temperature), Hottest Region and Date
maxtemp_df.show(1)


# In[37]:


#Hotest day Temperature
maxtemp_df.select("ScreenTemperature").show(1)


# In[38]:


#hottest day (yyyy-mm-dd HH:mm:ss)
maxtemp_df.select("ObservationDate").show(1)


# In[39]:


#hottest Region
maxtemp_df.select("Region").show(1)


# In[ ]:




