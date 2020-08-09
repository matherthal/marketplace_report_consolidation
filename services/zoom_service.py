import logging
import pandas as pd

from utils import convert_currency_br_to_num

_LOGGER = logging.getLogger(__file__)

def get_marketplace_data(report_path):
    mp_df = None
    for report in (report_path / 'marketplace').glob('*.csv'):
        temp_df = pd.read_csv(report, sep=',')
        
        mp_df = temp_df if mp_df is None else pd.concat([mp_df, temp_df])
    return mp_df

def process_report(mp_df):
    """
    - This report file doesn't contain "frete" data
    - The value is splitted in installments, so I multiplied it by the value and I set it to "total"
    """
    mp_df.dropna(subset=['Número do Pedido'], inplace=True)

    mp_df['Número do Pedido'] = mp_df['Número do Pedido'].astype(int).astype(str)

    mp_df['Valor do Ciclo'] = convert_currency_br_to_num(mp_df['Valor do Ciclo'])
    mp_df['Valor Comissão'] = convert_currency_br_to_num(mp_df['Valor Comissão'])
    mp_df['Valor de Repasse'] = convert_currency_br_to_num(mp_df['Valor de Repasse'])

    return mp_df

def consolidate(gp_fup_df, gp_mp_df):
    is_zoom = gp_fup_df['Marketplace']=='ZOOM'

    # gp_fup_df[is_carrefour].head()
    consolidated_df = pd.merge(
        gp_fup_df[is_zoom], 
        gp_mp_df, 
        how='outer', 
        left_on='Cod Pedido Comprador Num', 
        right_on='Número do Pedido'
    )
    
    consolidated_df['total igual'] = \
        (consolidated_df['Valor do Ciclo'] == consolidated_df['Total Filial FUP'])

    return consolidated_df
