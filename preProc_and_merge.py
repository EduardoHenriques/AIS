import pandas as pd
import requests
import time, os
from currency_conversion import currency_convert
"""
Python(pandas) script to preprocess and merge
the datasets for the Data Science project.
"""

start_time1 = time.time()


currency_codes = {
    'AFN': 'AFN', 'Lek': 'ALL', 'DZD': 'DZD', '$': 'USD', 'ARS': 'ARS',
    'AMD': 'AMD', 'man':  "AZN", '৳': 'BDT', 'BYN': 'BYN', 'CFA': 'XOF',
    'Nu.': 'BTN', '₹': 'INR', 'Bs.': 'BOB', 'P': 'BWP', 'R$': 'BRL', 'лв': 'BGN',
    'FBu': 'BIF', '¥': 'JPY', 'Col$': 'COP', '₡': 'CRC', 'Fdj': 'DJF', 'EC$': 'XCD',
    'RD$': 'DOP', 'FJ$': 'FJD', 'GEL': 'GEL', '₵': 'GHS', 'Q': 'GTQ', 'GY$': 'GYD',
    'Rp': 'IDR', 'IQD': 'IQD', 'J$': 'JMD', 'JOD': 'JOD', '₸': 'KZT', 'KSh': 'KES',
    'L£': 'LBP', 'R': 'ZAR', 'Ar': 'USD', 'MK': 'MKD', 'Rf': 'MVR', 'UM': 'MRO',
    'Rs': 'MUR', 'MXN': 'MXN', 'L': 'LKR', '₮': 'MNT', '€': 'EUR', 'MAD': 'MAD',
    'MT': 'MZN', 'N₨': 'NPR', '₦': 'NGN', 'ден': 'MKD',  'Rs.': 'PKR', 'K': 'PGK',
    'Gs': 'PYG', 'S/.': 'PEN', '₱': 'PHP', 'RF': 'MVR', 'Дин': 'RSD', 'SI$': 'SBD',
    'SRD': 'SRD', 'SM': 'SOS', 'TSh': 'TZS', '฿': 'THB', 'T$': 'TOP', 'DT': 'TND',
    'm': "TMT", 'USh': 'UGX', '₴': 'UAH', 'VT': 'VUV'
}





# Function to convert Excel to CSV using pandas
def convert_to_csv(input_file, output_file):
    df_excel = pd.read_excel(input_file)
    df_excel.to_csv(output_file, index=False)
    print("File converted -> .CSV")

# Function to download dataset using requests
def download_dataset(url, path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(path, 'wb') as f:
            f.write(response.content)
        print("File downloaded")
    else:
        print("Error-Code:", response.status_code)

# Download datasets
#download_dataset("https://api.data.apps.fao.org/api/v2/bigquery?sql_url=https://data.apps.fao.org/catalog/dataset/25f95b97-986d-4641-a244-3f0bb4348d91/resource/59e29af0-d4ff-4866-aeeb-8b5ad686fbda/download/datalab-parameterized-query.sql&object=01341",
#                 "src/Food_Prices.csv")

#convert_to_csv('src/Health_Nutrition.xlsx', 'src/Health_Nutrition.csv')

# Dataset 1 - DIVIDA EXTERNA
df1 = pd.read_csv("src/IDS_ALLCountries_Data.csv", encoding='windows-1252')
df1 = df1[['Country Name', 'Country Code', 'Counterpart-Area Name', 'Counterpart-Area Code', 'Series Name', 'Series Code', '2020', '2021', '2022']]
df1 = df1.dropna()

df1 = pd.melt(df1, id_vars=['Country Name', 'Country Code', 'Counterpart-Area Name', 'Counterpart-Area Code', 'Series Name', 'Series Code'], var_name='Year', value_name='Value')
df1 = df1[df1['Value'] != 0]
df1.rename(columns={'Value': 'Amount', 
                    'Country Code': 'Debt Country Code', 
                    'Series Name': 'Series Name[External Debt]', 
                    'Series Code': 'Series Code[External Debt]'}, inplace=True)
df1.drop(columns=['Debt Country Code'], inplace=True)
df1.to_csv('src/Divida_Externa.csv', index=False)
print("Dataset 1 Processed")

# Dataset 2 - SAUDE E NUTRICAO
df2 = pd.read_csv('src/Health_Nutrition.csv', encoding='windows-1252')
df2 = df2[df2['Series Name'] != 'Prevalence of stunting, height for age (% of children under 5)']

exclude_years = ['2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']
df2 = df2.drop(columns=[col_name for col_name in df2.columns if any(year in col_name for year in exclude_years)])
df2 = df2.dropna()

df2 = pd.melt(df2, id_vars=df2.columns[:4], value_vars=['2020 [YR2020]', '2021 [YR2021]', '2022 [YR2022]'], var_name='Year', value_name='Value')
df2 = df2[df2['Value'] != 0]

# Remove non-numeric characters from the 'Year' column
df2['Year'] = df2['Year'].str.extract(r'(\d+)')

# Convert the 'Year' column to numeric type
df2['Year'] = pd.to_numeric(df2['Year'])

df2 = df2.rename(columns={'Value': 'Amount', 'Series Name': 'Series Name[Health & Nutrition]', 'Series Code': 'Series Code[Health & Nutrition]'})
df2.to_csv('src/Saude_Nutricao.csv', index=False)
print("Dataset 2 Processed")

# Dataset 3 - PRECOS COMIDA
df3 = pd.read_csv('src/Food_Prices.csv', encoding='utf-8')
df3 = df3[~df3['Date'].str.contains('2023|2024')]
df3['Year'] = df3['Date'].str[:4]
df3 = df3.drop(columns=['Date'])
df3 = df3.dropna()

mean_by_year_country = df3.groupby(['country', 'Year']).agg({'local_price': 'mean'}).reset_index()
mean_by_year_country = mean_by_year_country.rename(columns={'local_price': 'local_price_mean'})
mean_by_year_country['local_price_mean'] = mean_by_year_country['local_price_mean'].round(2)

df3 = df3.drop_duplicates(subset=['country', 'Year']).sort_values(by=['country'])
df3 = df3.merge(mean_by_year_country, on=['country', 'Year'], how='inner')
df3 = df3.rename(columns={'country': 'Country Name'})
df3.to_csv('src/Precos_Comida.csv', index=False)
print("Dataset 3 Processed")

end_time1 = time.time()

# Calculate the elapsed time
elapsed_time1 = end_time1 - start_time1

print(f"Tempo pré-processamento {elapsed_time1}")

start_time2 = time.time()

# Merging and saving using pandas
df1 = pd.read_csv('src/Precos_Comida.csv')
df2 = pd.read_csv('src/Saude_Nutricao.csv')
df3 = pd.read_csv('src/Divida_Externa.csv')

# Renaming columns for consistency
df1 = df1.rename(columns={'country name': 'Country'})
df2 = df2.rename(columns={'country name': 'Country'})
df3 = df3.rename(columns={'country name': 'Country'})

# Convert 'Year' column in df2 to int
df2['Year'] = df2['Year'].astype(int)

# Merging
merged_df = pd.merge(df1, df2, on=['Year', 'Country Name'])
merged_df = pd.merge(merged_df, df3, on=['Year', 'Country Name'])

# Save merged dataframe to CSV
merged_df.rename(columns = {'Amount_x': 'Value[DEBT]', 'Amount_y': 'Value[HEALTH]'}, inplace = True)
merged_df.drop(columns= ['Country Code'], inplace=True)
merged_df = merged_df[~merged_df.eq('..').any(axis=1)] # Remove rows with '..' values
merged_df['Value[DEBT]'] = pd.to_numeric(merged_df['Value[DEBT]'], errors='coerce')

# print(merged_df.shape)
merged_df['local_currency'] = merged_df['local_currency'].map(currency_codes)
merged_df = currency_convert(merged_df)
# print(merged_df.shape)

merged_df.to_csv('dataset/Merged.csv', index=False)
# merged_df['Value[DEBT]'] = merged_df['Value[DEBT]'].astype(float)
print("Merged Dataset Processed")

end_time2 = time.time()

# Calculate the elapsed time
elapsed_time2 = end_time2 - start_time2

print(f"Tempo merge {elapsed_time2}")

merged_df.info()
# print(merged_df.head())

# PARA IMPORTAR PARA O CASSANDRA:
 #   Column                           Dtype           Cassandra 
# ---  ------                           -----          ---------
#  -1  ID                                NA             UUID 
#  0   Country Name                     object          TEXT
#  1   iso3                             object          TEXT
#  2   label                            object          TEXT 
#  3   baseline_local                   float64         DOUBLE
#  4   local_price                      float64         DOUBLE
#  5   variation_local                  float64         DOUBLE
#  6   local_currency                   object          TEXT
#  7   local_range                      object          TEXT
#  8   Year                             int64           INT  
#  9   local_price_mean                 float64         DOUBLE
#  10  Series Name[Health & Nutrition]  object          TEXT   
#  11  Series Code[Health & Nutrition]  object          TEXT
#  12  Value[DEBT]                      float64          DOUBLE 
#  13  Counterpart-Area Name            object          TEXT
#  14  Counterpart-Area Code            object          TEXT 
#  15  Series Name[External Debt]       object          TEXT 
#  16  Series Code[External Debt]       object          TEXT 
#  17  Value[HEALTH]                    float64         DOUBLE






