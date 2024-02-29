import pandas as pd
import requests
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, avg, round, sum, explode, array, when
import os

def convert_to_csv(input_file, output_file):
    df_excel = pd.read_excel(input_file)
    df_excel.to_csv(output_file, index=False)
    print("Ficheiro convertido -> .CSV")
    
    
def download_dataset(url, path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(path, 'wb') as f:
            f.write(response.content)
        print("File downloaded")
    else:
        print("Erro-Código:", response.status_code)
        
if __name__ == "__main__":
    # Spark Config
    conf = SparkConf()
    conf.set("spark.driver.port", "4044")  # replace 4040 with your desired port
    sc = SparkContext(conf=conf)
    # Spark Session
    spark = SparkSession.builder \
    .appName("DataProcessing") \
    .config("spark.driver.memory", "16g").getOrCreate()

    
    spark_home = os.environ.get('SPARK_HOME')

    # Print the value of SPARK_HOME
    print("SPARK_HOME:", spark_home)
    
    #
    # DATASET 1 - DIVIDA EXTERNA
    #
    df = spark.read.csv("src/IDS_ALLCountries_Data.csv", header=True, inferSchema=True)
    df = df.select(["Country Name","Country Code","Counterpart-Area Name","Counterpart-Area Code","Series Name","Series Code","2020","2021","2022"])
    df = df.dropna()
    
    # MERGE DE COLUNAS (2020,2021,2020) PARA COLUNAS (Year, Value)
    df = df.selectExpr(
    "*",
    "stack(3, '2020', 2020, '2021', 2021, '2022', 2022) as (Year, Value)"
    ).filter("Value != 0")  # Filter out rows where the value is zero
    
    df =  df.withColumn(
    "Value",
    when(col("Year") == 2020, col("2020"))
    .when(col("Year") == 2021, col("2021"))
    .when(col("Year") == 2022, col("2022"))
    .otherwise(col("Value"))  # Keep the original value if the year doesn't match
    )
    df = df.drop('2020', '2021', '2022')
    df = df.withColumnRenamed("Value", "Ammount")
    df = df.withColumnRenamed("Country Code", "Debt Country Code")
    df = df.withColumnRenamed("Series Name", "Series Name[External Debt]")
    df = df.withColumnRenamed("Series Code", "Series Code[External Debt]")
    df.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('dataset/Divida_Externa.csv',header = 'true')
    print("Dataset 1 Processed")
    
    #
    # DATASET 2 - SAUDE E NUTRICAO
    #
    # convert_to_csv('src/Health_Nutrition.xlsx', 'src/Health_Nutrition.csv')
    
    df2 = spark.read.format('com.databricks.spark.csv').load('src/Health_Nutrition.csv', header=True, inferSchema=True)
    df2 = df2.filter(df2['Series Name'] != 'Prevalence of stunting, height for age (% of children under 5)')
    
    exclude_years = ['2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']
    for year in exclude_years:
        df2 = df2.drop(*[col_name for col_name in df2.columns if year in col_name])
    df2 = df2.dropna()
    # Rename Columns
    df2 = df2.withColumnRenamed("2020 [YR2020]", "2020") \
            .withColumnRenamed("2021 [YR2021]", "2021") \
            .withColumnRenamed("2022 [YR2022]", "2022")
    
    # MERGE DE COLUNAS (2020,2021,2020) PARA COLUNAS (Year, Value)
    df2 = df2.selectExpr(
    "*",
    "stack(3, '2020', 2020, '2021', 2021, '2022', 2022) as (Year, Value)"
    ).filter("Value != 0")
    df2 = df2.withColumn(
        "Value",
        when(col("Year") == 2020, col("2020"))
        .when(col("Year") == 2021, col("2021"))
        .when(col("Year") == 2022, col("2022"))
        .otherwise(col("Value"))
    )
    df2 = df2.drop('2020', '2021', '2022')
    df2 = df2.withColumnRenamed("Series Name", "Series Name[Health & Nutrition]")
    df2 = df2.withColumnRenamed("Series Code", "Series Code[Health & Nutrition]")
    df2.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('dataset/Saude_Nutricao.csv',header = 'true')
    print("Dataset 2 Processed")
    
    #
    #DATASET 3 - PRECOS COMIDA
    #
    
    # download_dataset("https://api.data.apps.fao.org/api/v2/bigquery?sql_url=https://data.apps.fao.org/catalog/dataset/25f95b97-986d-4641-a244-3f0bb4348d91/resource/59e29af0-d4ff-4866-aeeb-8b5ad686fbda/download/datalab-parameterized-query.sql&object=01341",
    #                  "src/Food_Prices.csv")
    
    df3 = spark.read.format('com.databricks.spark.csv').load('src/Food_Prices.csv', header=True, inferSchema=True)
    # Filter Date
    df3 = df3.filter(~col('Date').contains('2023') & ~col('Date').contains('2024'))
    # Add 'Year' column
    df3 = df3.withColumn('Year', col('Date').substr(1, 4))
    # Drop Date column
    df3 = df3.drop('Date')
    # Remove missing values
    df3 = df3.dropna()
    # Group by country and year
    mean_by_year_country = df3.groupby('country', 'Year').agg(avg('local_price').alias('local_price_mean'))
    # Cast to double
    mean_by_year_country = mean_by_year_country.withColumn('local_price_mean', mean_by_year_country['local_price_mean'].cast('double'))
    # Round the values up to 2 decimal places
    mean_by_year_country = mean_by_year_country.withColumn('local_price_mean', round('local_price_mean', 2))
    # Remove duplicates
    df3 = df3.dropDuplicates(['country', 'Year'])
    # Order by country
    df3 = df3.orderBy('country', ascending=True)
    # Merge the two datasets
    df3 = df3.join(mean_by_year_country, on=['country', 'Year'], how='inner')
    # Rename Column for easy merge
    df3 = df3.withColumnRenamed("country", "Country Name")
    # Save the dataset
    df3.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('dataset/Precos_Comida.csv',header = 'true')
    print("Dataset 3 Processed")
    
    
    
    spark.stop()
    
