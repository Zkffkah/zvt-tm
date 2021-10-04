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
            StockActorSummary.record_data(entity_ids=entity_ids[0:], provider='em',sleeping_time=0.5)
            StockInstitutionalInvestorHolder.record_data(provider='em', sleeping_time=1)
            StockTopTenHolder.record_data(provider='em', sleeping_time=1)

            break
        except Exception as e:
            msg = f'em_data_runner error:{e}'
            logger.exception(msg)

            time.sleep(60)


if __name__ == '__main__':
    init_log('em_data_runner.log')

    run()

