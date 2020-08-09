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
    mp_df.dropna(subset=['N° do pedido'], inplace=True)

    mp_df.drop_duplicates(subset=['N° do pedido', 'Tipo', 'Quantidade.1'], inplace=True)
    mp_df['N° do pedido'] = mp_df['N° do pedido'].astype(str).apply(lambda n: n.split('-')[0])

    mp_df[['Débito', 'Crédito', 'Quantidade.1']] = \
        mp_df[['Débito', 'Crédito', 'Quantidade.1']].fillna(0.0)

    mp_df.rename(columns={'Quantidade.1': 'Pago', 'N° do pedido': 'Cod pedido'}, inplace=True)

    gp_mp_df = mp_df.pivot_table(
        index='Cod pedido', columns='Tipo', values='Pago', aggfunc=sum
    ).fillna(0.0).reset_index()

    gp_mp_df['percentual comissão sem frete'] = (
        (gp_mp_df['Taxas de comissão']) / gp_mp_df['Valor do pedido']).abs().round(4)

    gp_mp_df['percentual comissão com frete'] = (
        (gp_mp_df['Taxas de comissão']) / \
        (gp_mp_df['Valor do pedido'] + gp_mp_df['Valor do envio do pedido'])
    ).abs().round(4)

    return gp_mp_df

def consolidate(gp_fup_df, gp_mp_df):
    is_carrefour = gp_fup_df['Marketplace']=='CARREFOUR'

    # gp_fup_df[is_carrefour].head()
    consolidated_df = pd.merge(
        gp_fup_df[is_carrefour], 
        gp_mp_df, 
        how='outer', 
        left_on='Cod Pedido Comprador Num', 
        right_on='Cod pedido'
    )
    # len(consolidated_df['Cod Pedido Comprador Num'].unique())
    # len(gp_fup_df['Cod Pedido Comprador Num'].unique())
    # len(filial_vendas_df['Cod Pedido Comprador Num'].unique())
    # len(gp_mp_df['Cod pedido'].unique())
    # len(mp_df['Cod pedido'].unique())
    # len(consolidated_df['Cod pedido'].unique())
    consolidated_df['Valor do pedido'] = pd.to_numeric(consolidated_df['Valor do pedido'])

    consolidated_df[
        (~ consolidated_df['Cod Pedido Comprador'].isna())
        & (~ consolidated_df['Cod pedido'].isna())
    ]
    consolidated_df['total igual'] = \
        ( consolidated_df['Total com IPI'] == consolidated_df['Valor do pedido']) \
        & (consolidated_df['Frete'] == consolidated_df['Valor do envio do pedido'])

    # consolidated_df['faltando em Filial FUP'] = consolidated_df['Cod Pedido Comprador Num'].isna()
    # consolidated_df['faltando em carrefour'] = consolidated_df['Cod pedido'].isna()
    # consolidated_df.sum()
    # consolidated_df.columns
    # consolidated_df['Marketplace'] = consolidated_df['Marketplace'].fillna('CARREFOUR')
    # consolidated_df.groupby('Marketplace')[[
    #     'Cod Pedido Comprador', 'Total com IPI', 'Frete', 'Total Filial FUP', 
    #     'Valor do envio do pedido', 'Valor do pedido', 
    #     'total e frete corretos', 
    #     'faltando em Filial FUP', 'faltando em carrefour']].sum()

    # consolidated_df[['carrefour-Valor do pedido', 'Marketplace']].sum()
    # .groupby('Marketplace').sum()

    # consolidated_df.groupby('Marketplace').sum()

    return consolidated_df
