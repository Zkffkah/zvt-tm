# -*- coding: utf-8 -*-
import logging
import time

from zvt import init_log
from zvt.contract.api import get_entities
from zvt.domain import *

logger = logging.getLogger(__name__)

def run():
    while True:
        try:
            items = get_entities(entity_type='stock', provider='joinquant')
            entity_ids = items['entity_id'].to_list()
            # InstitutionalInvestorHolder.record_data(provider='eastmoney', sleeping_time=1)
            # HolderTrading.record_data(provider='eastmoney', sleeping_time=1)
            # TopTenHolder.record_data(provider='eastmoney', sleeping_time=1)
            # TopTenTradableHolder.record_data(provider='eastmoney', sleeping_time=1)
            # FinanceFactor.record_data(provider='eastmoney', sleeping_time=1)
            # BalanceSheet.record_data(provider='eastmoney', sleeping_time=1)
            # IncomeStatement.record_data(provider='eastmoney', sleeping_time=1)
            # CashFlowStatement.record_data(provider='eastmoney', sleeping_time=1)

            # StockInstitutionalInvestorHolder.record_data(provider='em', sleeping_time=2)
            StockActorSummary.record_data(entity_ids=entity_ids[3330:], provider='em', sleeping_time=1)
            StockInstitutionalInvestorHolder.record_data(provider='em', sleeping_time=1)
            StockTopTenHolder.record_data(provider='em', sleeping_time=1)
            StockTopTenFreeHolder.record_data(provider='em', sleeping_time=1)

            break
        except Exception as e:
            msg = f'eastmoney runner1 error:{e}'
            logger.exception(msg)

            time.sleep(60)


if __name__ == '__main__':
    init_log('eastmoney_data_runner1.log')

    run()

