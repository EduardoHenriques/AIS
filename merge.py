import pandas as pd
import requests
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, avg, round, sum, explode, array, when
import os


if __name__ == "__main__":
    # Spark Config
    conf = SparkConf()
    conf.set("spark.driver.port", "4044")  # replace 4040 with your desired port
    sc = SparkContext(conf=conf)
    # Spark Session
    spark = SparkSession.builder \
    .appName("DataProcessing") \
    .config("spark.driver.memory", "16g").getOrCreate()
    
    df1 = spark.read.format('com.databricks.spark.csv').load('dataset/Precos_Comida.csv', header=True, inferSchema=True)
    df2 = spark.read.format('com.databricks.spark.csv').load('dataset/Saude_Nutricao.csv', header=True, inferSchema=True)
    df3 = spark.read.format('com.databricks.spark.csv').load('dataset/Divida_Externa.csv', header=True, inferSchema=True)
    
    df1 = df1.withColumnRenamed('country name', 'Country')
    df2 = df2.withColumnRenamed('country name', 'Country')
    df3 = df3.withColumnRenamed('country name', 'Country')

    # Perform inner joins on 'Year' and 'Country' columns
    merged_df = df1.join(df2, ['Year', 'Country'], 'inner').join(df3, ['Year', 'Country'], 'inner')
    merged_df.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('dataset/Merged.csv',header = 'true')
    #merged_df = spark.read.format('com.databricks.spark.csv').load('dataset/Merged.csv', header=True, inferSchema=True)
    #merged_df.show()
    spark.stop()