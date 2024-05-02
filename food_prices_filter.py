import pandas as pd


df = pd.read_csv('dataset/Merged.csv')

# COMIDA

food = df[['Country Name', 'label', 'local_price', 'Year']]
food.drop_duplicates(inplace=True)
food.sort_values(by=['Country Name', 'Year'], inplace=True)

print(food.head())

debt = df[['Country Name','Series Name[External Debt]', 'Value[DEBT]', 'Year']]
debt.drop_duplicates(inplace=True)
debt.sort_values(by=['Country Name', 'Year'], inplace=True)
print(debt.head(100))

