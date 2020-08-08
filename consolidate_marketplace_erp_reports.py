#!/usr/bin/env python
# coding: utf-8

from datetime import datetime
import sys
import logging
import logging.config
import warnings

import pandas as pd
from pathlib import Path

from services import fup_service, b2w_service

logging.config.fileConfig('logging.conf')
warnings.filterwarnings("ignore")

_LOGGER = logging.getLogger()

_MARKETPLACE_SERVICES = {
    'b2w': b2w_service
}

def main(marketplace=None, report_date=None):
    _LOGGER.info(f'CONSOLIDAÇÃO DE {marketplace} REFERENTE À {report_date}')

    marketplace = marketplace.lower()

    report_path = Path(__file__).parent / 'reports' / marketplace / report_date

    fup_df = fup_service.get_fup_df(report_path)
    
    gp_fup_df = fup_service.process_fup_df(fup_df)

    fup_service.print_stats(gp_fup_df)

    if marketplace not in _MARKETPLACE_SERVICES:
        _LOGGER.error('MARKETPLACE DESCONHECIDO')
    mp_service = _MARKETPLACE_SERVICES[marketplace]

    mp_df = mp_service.get_marketplace_data(report_path)

    gp_mp_df = mp_service.process_report(mp_df)

    consolidated_df = mp_service.consolidate(gp_fup_df, gp_mp_df)
    
    report_path = report_path / 'consolidated.xlsx'
    consolidated_df.to_excel(report_path)

    _LOGGER.info(f'CAMINHO DO ARQUIVO CONSOLIDADO GERADO: {report_path}')
    _LOGGER.info('SCRIPT FINALIZADO COM SUCESSO')

if __name__ == "__main__":
    main(*sys.argv[1:])
