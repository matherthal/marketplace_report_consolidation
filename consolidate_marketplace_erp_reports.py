#!/usr/bin/env python
# coding: utf-8

from datetime import datetime
import sys
import logging
import logging.config
import warnings
import shutil

import pandas as pd
from pathlib import Path

from services import fup_service, b2w_service, carrefour_service, viavarejo_service, \
                     colombo_service, zoom_service

logging.config.fileConfig('logging.conf')
warnings.filterwarnings("ignore")

_LOGGER = logging.getLogger()

_MARKETPLACE_SERVICES = {
    'b2w': b2w_service,
    'carrefour': carrefour_service,
    'viavarejo': viavarejo_service,
    'colombo': colombo_service,
    'zoom': zoom_service
}

_MONTHS = {
    1.0: 'JANEIRO',
    2.0: 'FEVEREIRO',
    3.0: 'MARÇO',
    4.0: 'ABRIL',
    5.0: 'MAIO',
    6.0: 'JUNHO',
    7.0: 'JULHO',
    8.0: 'AGOSTO',
    9.0: 'SETEMBRO',
    10.0: 'OUTUBRO',
    11.0: 'NOVEMBRO',
    12.0: 'DEZEMBRO'
}

def get_max_date_folder(report_path):
    report_date = None

    for date_folder in report_path.glob('./*'):
        # Update report_date folder if it's the first one found or it's a bigger date
        if date_folder.is_dir() and (report_date is None or date_folder.name > report_date):
            report_date = date_folder.name
    
    return report_date
    
def export_monthly_consolidated_files(consolidated_df, path, marketplace, report_date, 
                                      split_discrepant=True):
    path = path / 'resultado'

    # Clean up files destination
    try: 
        shutil.rmtree(path)
    except:
        pass
    finally: 
        path.mkdir(parents=True, exist_ok=True)
    
    # Breakdown data in months by the "Dt Abertura" from FUP report
    for month in consolidated_df['Dt Abertura'].dt.month.unique():
        month_df = consolidated_df[consolidated_df['Dt Abertura'].dt.month == month]

        month_name = _MONTHS[month] if month in _MONTHS else 'ERRO'

        if split_discrepant:
            is_equal_revenue = month_df['total igual']

            if sum(is_equal_revenue) > 0:
                destination = \
                    path / f'{report_date}-{marketplace}-{month_name}-consolidado-IGUAL.xlsx'
                month_df[is_equal_revenue].to_excel(destination)
                _LOGGER.info(f'CAMINHO DO ARQUIVO CONSOLIDADO GERADO: {destination}')
            
            if sum(is_equal_revenue) < len(month_df):
                destination = \
                    path / f'{report_date}-{marketplace}-{month_name}-consolidado-DIFERENTE.xlsx'
                month_df[~is_equal_revenue].to_excel(destination)
                _LOGGER.info(f'CAMINHO DO ARQUIVO CONSOLIDADO GERADO: {destination}')
        else:
            destination = path / f'{report_date}-{marketplace}-{month_name}-consolidado.xlsx'
            month_df.to_excel(destination)
            _LOGGER.info(f'CAMINHO DO ARQUIVO CONSOLIDADO GERADO: {destination}')

def main(marketplace=None, report_date=None, error_bad_lines=True):
    marketplace = marketplace.lower()

    report_path = Path(__file__).parent / 'reports' / marketplace
    if not report_date:
        report_date = get_max_date_folder(report_path)
    report_path = report_path / report_date

    _LOGGER.info(f'CONSOLIDAÇÃO DE {marketplace} REFERENTE À {report_date}')

    _LOGGER.info('OBTENDO ARQUIVOS FUP')
    fup_df = fup_service.get_fup_df(report_path, error_bad_lines=error_bad_lines)
    
    _LOGGER.info('PROCESSANDO ARQUIVOS FUP')
    gp_fup_df = fup_service.process_fup_df(fup_df)

    _LOGGER.info('ESTATÍSTICAS DE ARQUIVO FUP')
    fup_service.print_stats(gp_fup_df)

    if marketplace not in _MARKETPLACE_SERVICES:
        _LOGGER.error('MARKETPLACE DESCONHECIDO')
    mp_service = _MARKETPLACE_SERVICES[marketplace]

    _LOGGER.info(f'OBTENDO ARQUIVOS {marketplace}')
    mp_df = mp_service.get_marketplace_data(report_path)

    _LOGGER.info(f'PROCESSANDO ARQUIVOS {marketplace}')
    gp_mp_df = mp_service.process_report(mp_df)

    _LOGGER.info(f'CONSOLIDANDO ARQUIVOS FUP E {marketplace}')
    consolidated_df = mp_service.consolidate(gp_fup_df, gp_mp_df)
    
    _LOGGER.info('EXPORTANDO ARQUIVO CONSOLIDADO PARA EXCEL COM A DATA DE HOJE')
    export_monthly_consolidated_files(consolidated_df, report_path, marketplace, report_date)

    _LOGGER.info('SCRIPT FINALIZADO COM SUCESSO')

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print('Consolidação de relatórios de Marketplaces e ERP -- Menu')

        options = ', '.join(_MARKETPLACE_SERVICES.keys())
        print(f'Digite o nome do marketplace que deseja processar dentre as opçÕes ({options}):')
        mp_selected = sys.stdin.readline().strip()

        print('(OPCIONAL) Digite a pasta com a data para a qual deseja processar relatórios')
        print('(se não digitar nada, será utilizada a última data):')
        date_selected = sys.stdin.readline().strip()
        
        print('(OPCIONAL) Digite "ignorar" para ignorar linhas dos relatórios que possuem erros, ou'
              ' apenas dê enter para prosseguir:')
        error_bad_lines = sys.stdin.readline().strip().lower() != 'ignorar'
        ans = 'NÃO ' if error_bad_lines else ''
        print(f'Foi escolhido {ans}IGNORAR linhas que possuam erros (por ex., número de colunas '
              'inválido)')
        
        main(mp_selected, date_selected, error_bad_lines)

        sys.stdin.readline()
    else:
        main(*sys.argv[1:])
