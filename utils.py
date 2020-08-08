import pandas as pd

def convert_currency_br_to_num(currency_col):
    return pd.to_numeric(currency_col.str.replace('.', '').str.replace(',', '.'))
