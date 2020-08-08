#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

def convert_currency_br_to_num(currency_col):
    return pd.to_numeric(currency_col.str.replace('.', '').str.replace(',', '.'))

def get_fup_df(report_path):
    fup_df = None
    for report in (report_path / 'fup').glob('*.csv'):
        if fup_df is None:
            fup_df = pd.read_csv(report, encoding='latin1', sep=';')
        else:
            fup_df = pd.concat([fup_df, pd.read_csv(report, encoding='latin1', sep=';')])
    return fup_df

def process_fup_df(fup_df):
    # Remove leading and trailling empty spaces from column names
    fup_df.columns = list(map(str.strip, fup_df.columns))

    fup_df['Cod Pedido Comprador'] = fup_df['Cod Pedido Comprador'].fillna('')
    
    fup_df['Cod Pedido Comprador'].apply(lambda s: str(s).split('-')[0]).unique()

    fup_df['Marketplace'] = fup_df['Cod Pedido Comprador'].apply(
        lambda c: c.split('-')[0].strip().upper()
    )

    # Extraindo código de nome do pedido
    # Carrefour é diferente pois tem sempre um "-A" on final
    fup_df['Cod Pedido Comprador Num'] = fup_df['Cod Pedido Comprador'].apply(
        lambda c: c.split('-')[-1] if 'CARREFOUR' not in c else '-'.join(c.split('-')[1:])
    )

    filial_vendas_keys = [
        'Cod Pedido Comprador', 
        'Cod Pedido Comprador Num', 
        'Marketplace', 
        'Nota Fiscal', 
        # 'Cod Cliente', 
        # 'Cod Pedido Vendedor', 
        # 'Desc Comercial', 
        # 'Marca', 
        # 'Modelo', 
        'Dt Abertura'
        # 'Dt Comprometida', 
        # 'Dt Faturamento', 
        # 'Dt Aprovação'
    ]
    filial_vendas_merics = ['Total com IPI', 'Frete']

    # Remove NA records
    fup_df = fup_df[~ fup_df['Cod Pedido Comprador'].isna()]

    # Drop unnecessary columns
    fup_df = fup_df[filial_vendas_keys + filial_vendas_merics]

    fup_df['Total com IPI'] = convert_currency_br_to_num(fup_df['Total com IPI'])
    fup_df['Frete'] = convert_currency_br_to_num(fup_df['Frete'])

    fup_df['Dt Abertura'] = pd.to_datetime(fup_df['Dt Abertura'], format='%d/%m/%Y')

    # fup_df[
    #     fup_df['Cod Pedido Comprador Num'].isin([
    #         # Estes códigos não estão batendo com o outro arquivo
    #         '274411276701', '274489181201', '108070233501',
    #         # Estes códigos tem mais de uma linha (código duplicado)
    #         '274411276701', '274489181201', '108076420901', '274192674501', '351721298101'
    #     ])].sort_values(by='Cod Pedido Comprador Num')
    # '274192674501', '274411276701', '274489181201', '108076420901', '351721298101'

    gp_fup_df = fup_df.groupby(
        filial_vendas_keys, as_index=False)[filial_vendas_merics].sum()

    # **Importante!** Incluindo todas as colunas do group by gera registros idesejáveis, como pode-se ver acima
    gp_fup_df['Total Filial FUP'] = gp_fup_df['Total com IPI'] + gp_fup_df['Frete']

    return gp_fup_df

def get_marketplace_data(report_path, marketplace):
    mp_path = report_path / 'marketplace'


def main(marketplace=None, report_date=None):
    report_path = Path(__file__).parent / 'reports' / marketplace / report_date

    fup_df = process_fup_df(get_fup_df(report_path))

    print('TOTAL POR MARKETPLACE:')
    print(fup_df.groupby(['Marketplace'])[['Total com IPI', 'Frete']].sum())

    duplicated_codes =fup_df.loc[
        fup_df['Cod Pedido Comprador Num'].duplicated(), 
        'Cod Pedido Comprador Num'
    ].unique()
    print(f'DUPLICATAS DE Cod Pedido Comprador Num: {duplicated_codes}')

    mp_data = get_marketplace_data(report_path, marketplace)

if __name__ == "__main__":
    main(*sys.argv[1:])

# extream_df = pd.read_csv(PATH / '20200601_B2WExtream_ReportCC12037195000395B2W_PARCEIRO_98FE50DD21C144F0A585244647 - 12037195000395.csv', 
#                          encoding='latin1', sep=';')

# extream_df = pd.concat([
#     extream_df,
#     pd.read_csv(PATH / '20200601_B2WTetrix_ReportCC12037195000123B2W_PARCEIRO_26B93342D8BA49E2A588102867 - 12037195000123.csv', 
#                 encoding='latin1', sep=';')
# ])
# # extream_df = pd.read_csv(PATH / '20200601_B2WTetrix_ReportCC12037195000123B2W_PARCEIRO_26B93342D8BA49E2A588102867 - 12037195000123.csv', 
# #                 encoding='latin1', sep=';')


# # In[27]:


# extream_keys = [
#     'Nome Fantasia', 'Data pedido', 'Data Pagamento', 'Data Estorno', 
#     'Data Liberação', 'Data Prevista Pgto', 'Ref. Pedido', 'Entrega'
# ]

# extream_lancamento = ['Lançamento']

# extream_value = ['Valor']


# # In[28]:


# extream_df = extream_df[extream_keys + extream_lancamento + extream_value]


# # In[29]:


# extream_df.dropna(subset=['Entrega'], inplace=True)


# # In[30]:


# extream_df['Ref. Pedido'] = extream_df['Ref. Pedido'].astype(str)
# extream_df['Entrega'] = extream_df['Entrega'].astype(int).astype(str)


# # In[31]:


# extream_df['Valor'] = pd.to_numeric(extream_df['Valor'].str.replace('.', '').str.replace(',', '.'))


# # In[32]:


# extream_df['Data pedido'] = pd.to_datetime(extream_df['Data pedido'], format='%d/%m/%Y')


# # In[33]:


# extream_df[extream_keys + extream_lancamento] = extream_df[extream_keys + extream_lancamento].fillna('')


# # In[34]:


# extream_df.tail()


# # In[35]:


# gp_extream_df = extream_df.pivot_table(
#     index=extream_keys, columns=extream_lancamento[0], values=extream_value[0], aggfunc=sum)


# # In[36]:


# gp_extream_df = gp_extream_df.fillna(0.0)


# # In[37]:


# gp_extream_df.head(20)


# # In[38]:


# unique_lancamentos = extream_df['Lançamento'].unique()
# print(len(unique_lancamentos))

# unique_lancamentos


# # In[39]:


# # Diferença simétrica
# # Aqui mostra que tem as mesmos valores na coluna lançamento do df original e nas colunas do dataframe gerado
# set(gp_extream_df.columns) ^ set(unique_lancamentos)


# # In[40]:


# extream_df.groupby('Lançamento')[['Valor']].sum()


# # In[41]:


# gp_extream_df = gp_extream_df.reset_index()


# # In[42]:


# gp_extream_df = gp_extream_df.groupby([
# #     'Nome Fantasia', 'Data pedido', 'Data Pagamento', 'Data Estorno',
# #     'Data Liberação', 'Data Prevista Pgto', 
#     'Ref. Pedido', 'Entrega'], as_index=False).sum()


# # In[43]:


# # gp_extream_df['total'] = gp_extream_df['Venda Marketplace'] + gp_extream_df['Frete B2W Entrega']


# # #### Remover valores iguais a zero

# # In[44]:


# # gp_extream_df = gp_extream_df[gp_extream_df['Venda Marketplace'] > 0.0]


# # In[45]:


# # gp_extream_df.columns = ['extream-'+c for c in gp_extream_df.columns]


# # # Consolidação B2W

# # In[46]:


# gp_fup_df.head()


# # In[47]:


# is_b2w = gp_fup_df['Marketplace'].isin(['B2W_NEW_API', 'Lojas Americanas', 'Shoptime', 'Submarino'])

# consolidated_df = pd.merge(
#     gp_fup_df[is_b2w], 
#     gp_extream_df, 
#     how='outer', 
#     left_on='Cod Pedido Comprador Num', 
#     right_on='extream-Entrega')


# # In[ ]:


# has_filial_fup = ~consolidated_df['Total Filial FUP'].isna()
# has_b2w = ~consolidated_df['extream-Venda Marketplace'].isna()

# cols_comp_values= ['Cod Pedido Comprador', 'Total com IPI', 'Frete', 'Total Filial FUP', 
#                    'extream-Venda Marketplace', 'extream-Frete B2W Entrega']

# consolidated_df.loc[has_filial_fup & has_b2w, cols_comp_values].tail(40)


# # In[ ]:


# consolidated_df['Total Filial FUP'] = consolidated_df['Total Filial FUP'].round(2)
# consolidated_df['extream-Venda Marketplace'] = consolidated_df['extream-Venda Marketplace'].round(2)

# # consolidated_df['extream-total'] = consolidated_df['extream-total'].round(2)


# # In[ ]:


# consolidated_df['extream-total igual'] =     consolidated_df['Total Filial FUP'] == consolidated_df['extream-Venda Marketplace']


# # parece ter um problema com essa versão do Filial, pois se deduzir o frete do valor Venda Marketplace, há 342 registros iguais, e sem deduzir, há 24

# # In[ ]:


# consolidated_df['extream-total igual'].sum()


# # In[ ]:


# consolidated_df.loc[
#     ~ consolidated_df[['Total Filial FUP', 'extream-Venda Marketplace']].isna().any(axis=1),
#     ['Total Filial FUP', 'extream-Venda Marketplace', 'extream-total igual']]


# # #### Comparação de linhas q tem valores mas que os totais sejam diferentes

# # In[ ]:


# consolidated_df[
#     (~ consolidated_df[['Total Filial FUP', 'extream-Venda Marketplace']].isna().any(axis=1))
#     & (~ consolidated_df['extream-total igual'])]


# # #### Lista de códigos que tem valores mais que sejam diferentes nos 2 arquivos

# # In[ ]:


# consolidated_df.loc[
#     (~ consolidated_df[['Total Filial FUP', 'extream-Venda Marketplace']].isna().any(axis=1))
#     & (~ consolidated_df['extream-total igual']),
#     'Cod Pedido Comprador Num'].to_list()


# # In[ ]:


# temp_df = consolidated_df.loc[
#     (~ consolidated_df[['Total Filial FUP', 'extream-Venda Marketplace']].isna().any(axis=1))
#     & (~ consolidated_df['extream-total igual'])].copy()

# temp_df['extream-Venda Marketplace dobrado'] = (temp_df['extream-Venda Marketplace']*2).round(2)

# temp_df['extream-total igual'] = temp_df['extream-Venda Marketplace dobrado'] == temp_df['Total Filial FUP']

# temp_df[['Total Filial FUP', 'extream-Venda Marketplace dobrado', 'extream-Venda Marketplace', 'extream-total igual']]


# # In[ ]:


# total_registros = len(consolidated_df)
# num_total_igual = consolidated_df['extream-total igual'].sum()
# num_total_diff = (~ consolidated_df['extream-total igual']).sum()
# num_total_filial_na = consolidated_df['Total Filial FUP'].isna().sum()
# num_total_filial_not_na = (~ consolidated_df['Total Filial FUP'].isna()).sum()
# num_venda_marketplace_na = consolidated_df['extream-Venda Marketplace'].isna().sum()
# num_venda_marketplace_not_na = (~ consolidated_df['extream-Venda Marketplace'].isna()).sum()

# print(f'''
# num registros: {total_registros}
# num de registros de totais iguais: {num_total_igual}
# num de registros de totais diferentes: {num_total_diff}
# num de registros de Total Filial FUP vazio: {num_total_filial_na}
# num de registros de Total Filial FUP preenchido: {num_total_filial_not_na}
# num de registros de Venda Marketplace vazio: {num_venda_marketplace_na}
# num de registros de Venda Marketplace preenchido: {num_venda_marketplace_not_na}
# ''')


# # In[ ]:


# consolidated_df['faltando em Filial FUP'] = consolidated_df['Cod Pedido Comprador Num'].isna()
# consolidated_df['faltando em Extream'] = consolidated_df['extream-Entrega'].isna()


# # In[ ]:


# consolidated_df.sort_values(['extream-total igual', 'faltando em Filial FUP', 'faltando em Extream'])    .to_csv(PATH / 'consolidated_df.csv', sep='\t')

# consolidated_df.sort_values(['extream-total igual', 'faltando em Filial FUP', 'faltando em Extream'])    .to_excel(PATH / 'consolidated.xlsx')


# # In[ ]:


# consolidated_df.groupby('Marketplace')[cols_comp_values].sum()


# # In[ ]:


# consolidated_df['extream-total diferente'] = ~ consolidated_df['extream-total igual']


# # In[ ]:


# consolidated_df.groupby('Marketplace')[
#     cols_comp_values + ['extream-total igual', 'extream-total diferente', 
#                         'faltando em Filial FUP', 'faltando em Extream']].sum()


# # In[ ]:


# is_above_lower_limit = consolidated_df['Dt Abertura'] >= '2020-05-01'
# is_below_upper_limit = consolidated_df['Dt Abertura'] <= '2020-05-15'

# consolidated_df[is_above_lower_limit & is_below_upper_limit].groupby('Marketplace')[
#     cols_comp_values + ['extream-total igual', 'extream-total diferente', 
#                         'faltando em Filial FUP', 'faltando em Extream']].sum()
