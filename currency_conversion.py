import pandas as pd
import requests
import json


# API key (17dc639f8dd4fb018198b384)


def currency_convert(df):

    url = 'https://v6.exchangerate-api.com/v6/17dc639f8dd4fb018198b384/latest/USD'

    # Making our request
    response = requests.get(url)
    data = response.json()

    conversion_rates = data['conversion_rates']
    conversion_rates_keys = list(conversion_rates.keys())

    codes = ['AFN', 'ALL', 'DZD', 'USD', 'ARS', 'AMD', 'AZN', 'BDT', 'BYN', 'XOF', 'BTN', 'INR',
    'BOB', 'BWP', 'BRL', 'BGN', 'BIF', 'JPY', 'COP', 'CRC', 'DJF', 'XCD', 'DOP', 'FJD',
    'GEL', 'GHS', 'GTQ', 'GYD', 'IDR', 'IQD', 'JMD', 'JOD', 'KZT', 'KES', 'LBP', 'ZAR',
    'MKD', 'MVR', 'MRO', 'MUR', 'MXN', 'LKR', 'MNT', 'EUR', 'MAD', 'MZN', 'NPR', 'NGN',
    'PKR', 'PGK', 'PYG', 'PEN', 'PHP', 'RSD', 'SBD', 'SRD', 'SOS', 'TZS', 'THB', 'TOP',
    'TND', 'TMT', 'UGX', 'UAH', 'VUV']

    missing_codes = set(codes) - set(conversion_rates_keys)
    print(missing_codes)
    
    for code in missing_codes:
        df = df[df['local_currency'] != code]
    
    
    
    df.loc[:,'local_price'] = df['local_price'].astype(float) / df['local_currency'].map(conversion_rates)
    df.loc[:,'baseline_local'] = df['local_price'].astype(float) / df['local_currency'].map(conversion_rates)
    df.loc[:,'local_price_mean'] = df['local_price'].astype(float) / df['local_currency'].map(conversion_rates)
    return df
    
    


