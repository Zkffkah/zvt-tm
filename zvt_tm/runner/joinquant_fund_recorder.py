# -*- coding: utf-8 -*-
import logging
import time
from operator import or_

from apscheduler.schedulers.background import BackgroundScheduler
from zvt import init_log
from zvt.contract.api import get_entities
from zvt.domain import Fund, FundStock, Stock1wkHfqKdata, StockValuation, Stock
from zvt.informer.informer import EmailInformer
from zvt.utils import now_pd_timestamp

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


# 周6抓取
@sched.scheduled_job('cron', hour=10, minute=00, day_of_week=5)
def record_fund():
    while True:
        email_action = EmailInformer()

        try:
            # 基金和基金持仓数据
            Fund.record_data(provider='joinquant', sleeping_time=1)
            FundStock.record_data(provider='joinquant', sleeping_time=1)
            # 股票周线后复权数据
            Stock1wkHfqKdata.record_data(provider='joinquant', sleeping_time=0)

            # email_action.send_message("5533061@qq.com", 'joinquant record fund finished', '')
            break
        except Exception as e:
            msg = f'joinquant record fund error:{e}'
            logger.exception(msg)

            # email_action.send_message("5533061@qq.com", 'joinquant record fund error', msg)
            time.sleep(60)


# 周6抓取
@sched.scheduled_job('cron', hour=13, minute=00, day_of_week=6)
def record_valuation():
    while True:
        email_action = EmailInformer()

        try:
            items = get_entities(entity_type='stock', provider='joinquant',
                                 filters=[or_(Stock.end_date.is_(None), Stock.end_date > now_pd_timestamp())])
            entity_ids = items['entity_id'].to_list()
            # StockValuation.record_data(provider='joinquant', entity_ids=entity_ids[1:50], sleeping_time=0, day_data=True)
            StockValuation.record_data(provider='joinquant', entity_ids=entity_ids, sleeping_time=0, day_data=True)

            # email_action.send_message("5533061@qq.com", 'joinquant record valuation finished', '')
            break
        except Exception as e:
            msg = f'joinquant record valuation error:{e}'
            logger.exception(msg)

            # email_action.send_message("5533061@qq.com", 'joinquant record valuation error', msg)
            time.sleep(60)


if __name__ == '__main__':
    init_log('joinquant_fund_runner.log')

    record_fund()

    record_valuation()

    sched.start()

    sched._thread.join()
