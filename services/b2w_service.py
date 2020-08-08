import logging
import pandas as pd

from utils import convert_currency_br_to_num

_LOGGER = logging.getLogger(__file__)

def get_marketplace_data(report_path):
    mp_df = None
    for report in (report_path / 'marketplace').glob('*.csv'):
        if mp_df is None:
            mp_df = pd.read_csv(report, encoding='latin1', sep=';')
        else:
            mp_df = pd.concat([mp_df, pd.read_csv(report, encoding='latin1', sep=';')])
    return mp_df

def process_report(mp_df):
    key_cols = ['Nome Fantasia', 'Data pedido', 'Ref. Pedido', 'Entrega']
    metric_col = 'Lançamento'
    value_col = 'Valor'
    
    # Drop unnecessary columns
    mp_df = mp_df[key_cols + [metric_col] + [value_col]]

    # mp_df.dropna(subset=['Entrega'], inplace=True)
    
    mp_df['Ref. Pedido'] = mp_df['Ref. Pedido'].astype(str)

    mp_df['Entrega'].fillna(-1, inplace=True)
    mp_df['Entrega'] = mp_df['Entrega'].astype(int).astype(str)

    mp_df['Valor'] = convert_currency_br_to_num(mp_df['Valor'])
    mp_df['Data pedido'] = pd.to_datetime(mp_df['Data pedido'], format='%d/%m/%Y')

    # mp_df[key_cols + [metric_col]] = mp_df[key_cols + [metric_col]].fillna('', inplace=True)

    # Reshape DF to set the metric_cols as columns whose values are in value_col, the index is 
    # defined by the key_cols and the aggregate function is sum
    gp_mp_df = mp_df.pivot_table(
        index=key_cols, columns=metric_col, values=value_col, aggfunc=sum).reset_index()
    
    gp_mp_df = gp_mp_df.fillna(0.0)

    unique_lancamentos = ', '.join(mp_df['Lançamento'].unique())
    _LOGGER.info(f'TIPOS DE LANÇAMENTOS IDENTIFICADOS: {unique_lancamentos}')

    # Diferença simétrica
    # Aqui mostra que tem as mesmos valores na coluna lançamento do df original e nas colunas do dataframe gerado
    # set(gp_mp_df.columns) ^ set(unique_lancamentos)
    # mp_df.groupby('Lançamento')[['Valor']].sum()
    
    gp_mp_df = gp_mp_df.groupby(key_cols, as_index=False).sum()
    # gp_mp_df = gp_mp_df.groupby(['Ref. Pedido', 'Entrega'], as_index=False).sum()

    # gp_mp_df['total'] = gp_mp_df['Venda Marketplace'] + gp_mp_df['Frete B2W Entrega']

    # #### Remover valores iguais a zero
    # gp_mp_df = gp_mp_df[gp_mp_df['Venda Marketplace'] > 0.0]
    # gp_mp_df.columns = [''+c for c in gp_mp_df.columns]

    return gp_mp_df

def consolidate(gp_fup_df, gp_mp_df):
    # Consolidação B2W
    is_b2w = gp_fup_df['Marketplace'].isin([
        'B2W_NEW_API', 'LOJAS AMERICANAS', 'SHOPTIME', 'SUBMARINO'])

    consolidated_df = pd.merge(
        gp_fup_df[is_b2w], 
        gp_mp_df, 
        how='outer', 
        left_on='Cod Pedido Comprador Num', 
        right_on='Entrega')
    
    has_filial_fup = ~consolidated_df['Total Filial FUP'].isna()
    has_b2w = ~consolidated_df['Venda Marketplace'].isna()

    cols_comp_values= ['Cod Pedido Comprador', 'Total com IPI', 'Frete', 'Total Filial FUP', 
                       'Venda Marketplace', 'Frete B2W Entrega']

    # consolidated_df.loc[has_filial_fup & has_b2w, cols_comp_values].tail(40)

    consolidated_df['Total Filial FUP'] = consolidated_df['Total Filial FUP'].round(2)
    consolidated_df['Venda Marketplace'] = consolidated_df['Venda Marketplace'].round(2)

    # consolidated_df['total'] = consolidated_df['total'].round(2)
    consolidated_df['total igual'] = (
        consolidated_df['Total Filial FUP'] == consolidated_df['Venda Marketplace'])

    # parece ter um problema com essa versão do Filial, pois se deduzir o frete do valor Venda Marketplace, há 342 registros iguais, e sem deduzir, há 24
    num_total_igual = consolidated_df['total igual'].sum()
    _LOGGER.info(f'NÚMERO DE REGISTROS COM TOTAL IGUAL: {num_total_igual})')

    # consolidated_df.loc[
    #     ~ consolidated_df[['Total Filial FUP', 'Venda Marketplace']].isna().any(axis=1),
    #     ['Total Filial FUP', 'Venda Marketplace', 'total igual']]


    # #### Comparação de linhas q tem valores mas que os totais sejam diferentes
    # consolidated_df[
    #     (~ consolidated_df[['Total Filial FUP', 'Venda Marketplace']].isna().any(axis=1))
    #     & (~ consolidated_df['total igual'])]


    # #### Lista de códigos que tem valores mais que sejam diferentes nos 2 arquivos
    # consolidated_df.loc[
    #     (~ consolidated_df[['Total Filial FUP', 'Venda Marketplace']].isna().any(axis=1))
    #     & (~ consolidated_df['total igual']),
    #     'Cod Pedido Comprador Num'].to_list()
    # temp_df = consolidated_df.loc[
    #     (~ consolidated_df[['Total Filial FUP', 'Venda Marketplace']].isna().any(axis=1))
    #     & (~ consolidated_df['total igual'])].copy()

    # temp_df['Venda Marketplace dobrado'] = (temp_df['Venda Marketplace']*2).round(2)

    # temp_df['total igual'] = temp_df['Venda Marketplace dobrado'] == temp_df['Total Filial FUP']

    # temp_df[['Total Filial FUP', 'Venda Marketplace dobrado', 'Venda Marketplace', 'total igual']]
    # total_registros = len(consolidated_df)
    # num_total_igual = consolidated_df['total igual'].sum()
    # num_total_diff = (~ consolidated_df['total igual']).sum()
    # num_total_filial_na = consolidated_df['Total Filial FUP'].isna().sum()
    # num_total_filial_not_na = (~ consolidated_df['Total Filial FUP'].isna()).sum()
    # num_venda_marketplace_na = consolidated_df['Venda Marketplace'].isna().sum()
    # num_venda_marketplace_not_na = (~ consolidated_df['Venda Marketplace'].isna()).sum()

    # _LOGGER.info(f'''
    # num registros: {total_registros}
    # num de registros de totais iguais: {num_total_igual}
    # num de registros de totais diferentes: {num_total_diff}
    # num de registros de Total Filial FUP vazio: {num_total_filial_na}
    # num de registros de Total Filial FUP preenchido: {num_total_filial_not_na}
    # num de registros de Venda Marketplace vazio: {num_venda_marketplace_na}
    # num de registros de Venda Marketplace preenchido: {num_venda_marketplace_not_na}
    # ''')
    # consolidated_df['faltando em Filial FUP'] = consolidated_df['Cod Pedido Comprador Num'].isna()
    # consolidated_df['faltando em Extream'] = consolidated_df['Entrega'].isna()
    # consolidated_df.sort_values(['total igual', 'faltando em Filial FUP', 'faltando em Extream']).to_csv(PATH / 'consolidated_df.csv', sep='\t')

    # consolidated_df = consolidated_df.sort_values(['total igual', 'faltando em Filial FUP', 'faltando em Extream'])
    
    return consolidated_df
    
    # consolidated_df.groupby('Marketplace')[cols_comp_values].sum()
    # consolidated_df['total diferente'] = ~ consolidated_df['total igual']
    # consolidated_df.groupby('Marketplace')[
    #     cols_comp_values + ['total igual', 'total diferente', 
    #                         'faltando em Filial FUP', 'faltando em Extream']].sum()
    # is_above_lower_limit = consolidated_df['Dt Abertura'] >= '2020-05-01'
    # is_below_upper_limit = consolidated_df['Dt Abertura'] <= '2020-05-15'

    # consolidated_df[is_above_lower_limit & is_below_upper_limit].groupby('Marketplace')[
    #     cols_comp_values + ['total igual', 'total diferente', 
    #                         'faltando em Filial FUP', 'faltando em Extream']].sum()
