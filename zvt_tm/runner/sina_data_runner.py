# -*- coding: utf-8 -*-
import logging
import time


from zvt import init_log
from zvt.domain import *

logger = logging.getLogger(__name__)


def record_block():
    while True:

        try:
            Block.record_data(provider='sina', sleeping_time=1)
            BlockStock.record_data(provider='sina', sleeping_time=1)

            break
        except Exception as e:
            msg = f'sina block error:{e}'
            logger.exception(msg)

            time.sleep(60)


def record_money_flow():
    while True:

        try:
            BlockMoneyFlow.record_data(provider='sina', sleeping_time=1)

            break
        except Exception as e:
            msg = f'sina money flow error:{e}'
            logger.exception(msg)

            time.sleep(60)


if __name__ == '__main__':
    init_log('sina_data_runner.log')

    record_block()
    record_money_flow()
