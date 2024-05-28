
import pandas as pd
from currency_conversion import currency_convert

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

df = pd.read_csv("dataset/Merged.csv")
print(df['local_currency'].unique())