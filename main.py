import pandas as pd
import requests
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col


def convert_to_csv(input_file, output_file):
    df_excel = pd.read_excel(input_file)
    df_excel.to_csv(output_file, index=False)
    print("File converted to CSV")
    
    
def download_dataset(url, path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(path, 'wb') as f:
            f.write(response.content)
        print("File downloaded successfully.")
    else:
        print("Failed to download the file. Status code:", response.status_code)
        
if __name__ == "__main__":
    # Spark Config
    conf = SparkConf()
    conf.set("spark.driver.port", "4044")  # replace 4040 with your desired port
    sc = SparkContext(conf=conf)
    
    # Spark Session
    spark = SparkSession.builder.appName("DataProcessing") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.12.3") \
    .getOrCreate()
    
    # DATASET 1 - DIVIDA EXTERNA
    df = spark.read.csv("src/IDS_ALLCountries_Data.csv", header=True, inferSchema=True)
    df = df.select(["Country Name","Country Code","Counterpart-Area Name","Counterpart-Area Code","Series Name","Series Code","2017","2018","2019","2020","2021","2022","2023"])
    df = df.dropna()
    df.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('dataset/Divida_Externa.csv',header = 'true')
    print("Dataset 1 Processed")
    
    # DATASET 2 - SAUDE E NUTRICAO
    
    # convert_to_csv('src/Health_Nutrition.xlsx', 'src/Health_Nutrition.csv')
    
    df2 = spark.read.format('com.databricks.spark.csv').load('src/Health_Nutrition.csv', header=True, inferSchema=True)
    df2 = df2.filter(df2['Series Name'] != 'Prevalence of stunting, height for age (% of children under 5)')
    df2 = df2.dropna()
    df2.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('dataset/Saude_Nutricao.csv',header = 'true')
    print("Dataset 2 Processed")
    
    #DATASET 3 - PRECOS COMIDA
    
    # download_dataset("https://api.data.apps.fao.org/api/v2/bigquery?sql_url=https://data.apps.fao.org/catalog/dataset/25f95b97-986d-4641-a244-3f0bb4348d91/resource/59e29af0-d4ff-4866-aeeb-8b5ad686fbda/download/datalab-parameterized-query.sql&object=01341",
    #                  "src/Food_Prices.csv")
    
    df3 = spark.read.format('com.databricks.spark.csv').load('src/Food_Prices.csv', header=True, inferSchema=True)
    df3 = df2.filter(df2['Series Name'] != 'Prevalence of stunting, height for age (% of children under 5)')
    df3 = df2.dropna()
    df3.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('dataset/Precos_Comida.csv',header = 'true')
    print("Dataset 3 Processed")
        
    spark.stop()
