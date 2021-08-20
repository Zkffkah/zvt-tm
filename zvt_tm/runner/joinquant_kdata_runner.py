# -*- coding: utf-8 -*-
import logging
import time

from zvt import init_log
from zvt.contract.api import get_entities
from zvt.domain import *

logger = logging.getLogger(__name__)



def record_stock():
    while True:

        try:
            Stock.record_data(provider='joinquant', sleeping_time=1)
            StockTradeDay.record_data(provider='joinquant', sleeping_time=1)
            break
        except Exception as e:
            msg = f'joinquant record stock:{e}'
            logger.exception(msg)

            time.sleep(60 * 5)


def record_kdata():
    while True:
        try:
            # items = get_entities(entity_type='stock', provider='joinquant')
            # entity_ids = items['entity_id'].to_list()
            #
            # try:
            #     Stock1dKdata.record_data(provider='joinquant', entity_ids=entity_ids[4172:], sleeping_time=0.5)
            # except Exception as e:
            #     logger.exception('report_tm error:{}'.format(e))
            # 日线前复权和后复权数据
            # Stock1dKdata.record_data(provider='joinquant', sleeping_time=0)
            Stock1dHfqKdata.record_data(provider='joinquant', sleeping_time=0, day_data=True)
            # StockMoneyFlow.record_data(provider='joinquant', sleeping_time=0, day_data=True)
            # IndexMoneyFlow.record_data(provider='joinquant', sleeping_time=0, day_data=True)
            break
        except Exception as e:
            msg = f'joinquant record kdata:{e}'
            logger.exception(msg)

            time.sleep(60 * 5)


if __name__ == '__main__':
    init_log('joinquant_kdata_runner.log')
    record_stock()
    record_kdata()
