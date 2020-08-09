import logging
import pandas as pd

from utils import convert_currency_br_to_num

_LOGGER = logging.getLogger(__file__)

def get_fup_df(report_path, error_bad_lines=True):
    fup_df = None
    for report in (report_path / 'fup').glob('*.csv'):
        try:
            temp_df = pd.read_csv(report, encoding='latin1', sep=';', 
                                  error_bad_lines=bool(error_bad_lines))
            
            fup_df = temp_df if fup_df is None else pd.concat([fup_df, temp_df])
        except:
            _LOGGER.exception(f'ERRO AO PROCESSAR ARQUIVO: {report}')
            if error_bad_lines:
                raise
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
        'Dt Abertura'
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

def print_stats(gp_fup_df):
    _LOGGER.info('TOTAL POR MARKETPLACE:')
    _LOGGER.info(gp_fup_df.groupby(['Marketplace'])[['Total com IPI', 'Frete']].sum())

    duplicated_codes =gp_fup_df.loc[
        gp_fup_df['Cod Pedido Comprador Num'].duplicated(), 
        'Cod Pedido Comprador Num'
    ].unique()
    _LOGGER.info(f'DUPLICATAS DE Cod Pedido Comprador Num: {duplicated_codes}')
