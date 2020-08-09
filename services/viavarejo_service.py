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
    mp_df['Número do Pedido'] = mp_df['Número do Pedido'].astype(str)
    # mp_df['Tipo da Transação'].unique()
    # mp_df.pivot(index='Número do Pedido', columns=['Tipo da Transação'],Valor do Repasse )
    # mp_df[mp_df['Tipo da Transação']=='CANCELAMENTO']
    # mp_df[mp_df['Número do Pedido'].isin(
    # [
    # #     ajuste:
    # #     21373314701, 21372937301, 21113069101 , 20511448501, 20511838801, 20521177601
    #     '20827887101'
    # ]
    # )]


    # ##### Os casos de duplicatas nos códigos de pedido são referentes aos tipos de produto. 
    # Quando há cancelamento ou "ajustes" (comissões).
    # 
    # Há casos onde há cancelamento e não há a venda
    duplicated_pedidos = mp_df.loc[
        mp_df['Número do Pedido'].duplicated(), 'Número do Pedido'].unique()

    # mp_df[mp_df['Número do Pedido'].isin(duplicated_pedidos)].sort_values('Número do Pedido').tail(50)
    val_cols = ['Valor da Transação', 'Valor da Comissão Aplicado Via Varejo', 'Valor do Repasse']

    for val_col in val_cols:
        mp_df[val_col] = pd.to_numeric(mp_df[val_col].str.replace(',', '.'))
    
    num_cols = [
        'Valor da Transação', 
        'Valor da Comissão Aplicado Via Varejo', 
        'Valor da Comissão original', 
        'Valor do Repasse'
    ]

    gp_mp_df = mp_df.groupby('Número do Pedido', as_index=False)[num_cols].sum()
    return gp_mp_df

def consolidate(gp_fup_df, gp_mp_df):
    consolidated_df = pd.merge(
        gp_fup_df, 
        gp_mp_df, 
        how='outer',
        left_on='Cod Pedido Comprador Num', 
        right_on='Número do Pedido')

    consolidated_df = consolidated_df[~consolidated_df['Número do Pedido'].isna()]
    # consolidated_df['total igual'] = (consolidated_df['Total com IPI'].abs() == consolidated_df['Valor do Repasse'].abs())
    consolidated_df['total igual'] = (
        consolidated_df['Total Filial FUP'].abs() == consolidated_df['Valor do Repasse'].abs())

    return consolidated_df

# def print_stats(consolidated_df):
#     # #### Registros vazios em df1 e 2
#     (consolidated_df['Cod Pedido Comprador'].isna().sum(), consolidated_df['Número do Pedido'].isna().sum())


#     # #### Registros não vazios em df1 e 2
#     ((~consolidated_df['Cod Pedido Comprador'].isna()).sum(), (~consolidated_df['Número do Pedido'].isna()).sum())


#     # #### Registros presentes em ambos os dfs
#     present_both = ((~consolidated_df['Cod Pedido Comprador'].isna()) & (~consolidated_df['Número do Pedido'].isna()))

#     present_both.sum()


#     # #### Contagem de registros com totais iguals
#     consolidated_df['total igual'].sum()
