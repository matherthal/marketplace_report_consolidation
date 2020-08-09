import logging
import pandas as pd

from utils import convert_currency_br_to_num

_LOGGER = logging.getLogger(__file__)

def get_marketplace_data(report_path):
    mp_df = None
    for report in (report_path / 'marketplace').glob('*.csv'):
        temp_df = pd.read_csv(report, sep=';')
        
        mp_df = temp_df if mp_df is None else pd.concat([mp_df, temp_df])
    return mp_df

def process_report(mp_df):
    """
    This report file doesn't contain "frete" data
    """
    mp_df.dropna(subset=['Número da Entrega'], inplace=True)

    mp_df['Número do pedido'] = mp_df['Número do pedido'].astype(str)
    mp_df['Número da Entrega'] = mp_df['Número da Entrega'].astype(str)

    key_cols = ['Número do pedido', 'Número da Entrega', 'Data do Pedido']
    gp_mp_df = mp_df.pivot_table(
        index=key_cols, columns='Tipo Lançamento', values='Valor', aggfunc=sum
    ).fillna(0.0).reset_index()

    gp_mp_df['percentual comissão'] = (
        (gp_mp_df['Comissão']) / gp_mp_df['Venda Marketplace']).abs().round(4)

    return gp_mp_df

def consolidate(gp_fup_df, gp_mp_df):
    is_carrefour = gp_fup_df['Marketplace']=='COLOMBO'

    # gp_fup_df[is_carrefour].head()
    consolidated_df = pd.merge(
        gp_fup_df[is_carrefour], 
        gp_mp_df, 
        how='outer', 
        left_on='Cod Pedido Comprador Num', 
        right_on='Número da Entrega'
    )
    # len(consolidated_df['Cod Pedido Comprador Num'].unique())
    # len(gp_fup_df['Cod Pedido Comprador Num'].unique())
    # len(filial_vendas_df['Cod Pedido Comprador Num'].unique())
    # len(gp_mp_df['Cod pedido'].unique())
    # len(mp_df['Cod pedido'].unique())
    # len(consolidated_df['Cod pedido'].unique())
    consolidated_df['Venda Marketplace'] = pd.to_numeric(consolidated_df['Venda Marketplace'])

    consolidated_df[
        (~ consolidated_df['Cod Pedido Comprador'].isna())
        & (~ consolidated_df['Número da Entrega'].isna())
    ]
    consolidated_df['total igual'] = \
        ( consolidated_df['Total Filial FUP'] == consolidated_df['Venda Marketplace'])

    # consolidated_df['faltando em Filial FUP'] = consolidated_df['Cod Pedido Comprador Num'].isna()
    # consolidated_df['faltando em carrefour'] = consolidated_df['Cod pedido'].isna()
    # consolidated_df.sum()
    # consolidated_df.columns
    # consolidated_df['Marketplace'] = consolidated_df['Marketplace'].fillna('CARREFOUR')
    # consolidated_df.groupby('Marketplace')[[
    #     'Cod Pedido Comprador', 'Total com IPI', 'Frete', 'Total Filial FUP', 
    #     'Valor do envio do pedido', 'Venda Marketplace', 
    #     'total e frete corretos', 
    #     'faltando em Filial FUP', 'faltando em carrefour']].sum()

    # consolidated_df[['carrefour-Venda Marketplace', 'Marketplace']].sum()
    # .groupby('Marketplace').sum()

    # consolidated_df.groupby('Marketplace').sum()

    return consolidated_df
