# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from zvt import init_log
from zvt.domain import *
from zvt.informer.informer import EmailInformer

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour=15, minute=30, day_of_week=3)
def record_block():
    while True:
        # email_action = EmailInformer()

        try:
            Block.record_data(provider='sina')
            BlockStock.record_data(provider='sina')

            # email_action.send_message("5533061@qq.com", 'sina block finished', '')
            break
        except Exception as e:
            msg = f'sina block error:{e}'
            logger.exception(msg)

            # email_action.send_message("5533061@qq.com", 'sina block error', msg)
            time.sleep(60)


@sched.scheduled_job('cron', hour=15, minute=30)
def record_money_flow():
    while True:
        # email_action = EmailInformer()

        try:
            BlockMoneyFlow.record_data(provider='sina')

            # email_action.send_message("5533061@qq.com", 'sina money flow finished', '')
            break
        except Exception as e:
            msg = f'sina money flow error:{e}'
            logger.exception(msg)

            # email_action.send_message("5533061@qq.com", 'sina money flow error', msg)
            time.sleep(60)
# 周6抓取
@sched.scheduled_job('cron', hour=2, minute=00, day_of_week=5)
def record_wkdata():
    while True:
        email_action = EmailInformer()

        try:
            # 周线前复权和后复权数据
            Stock1wkKdata.record_data(provider='joinquant', sleeping_time=1)
            Stock1wkHfqKdata.record_data(provider='joinquant', sleeping_time=1)
            # 个股估值数据
            StockValuation.record_data(provider='joinquant', sleeping_time=1)


            # email_action.send_message("5533061@qq.com", 'joinquant record week kdata finished', '')
            break
        except Exception as e:
            msg = f'joinquant record kdata:{e}'
            logger.exception(msg)

            # email_action.send_message("5533061@qq.com", 'joinquant record week kdata error', msg)
            time.sleep(60)


@sched.scheduled_job('cron', hour=6, minute=0)
def record_stock():
    while True:
        email_action = EmailInformer()

        try:
            Stock.record_data(provider='joinquant', sleeping_time=1)
            StockTradeDay.record_data(provider='joinquant', sleeping_time=1)
            # email_action.send_message("5533061@qq.com", 'joinquant record stock finished', '')
            break
        except Exception as e:
            msg = f'joinquant record stock:{e}'
            logger.exception(msg)

            email_action.send_message("5533061@qq.com", 'joinquant record stock error', msg)
            time.sleep(60)


@sched.scheduled_job('cron', hour=15, minute=20)
def record_dkdata():
    while True:
        email_action = EmailInformer()

        try:
            # 日线前复权和后复权数据
            Stock1dKdata.record_data(provider='joinquant', sleeping_time=1)
            # Stock1dHfqKdata.record_data(provider='joinquant', sleeping_time=1)
            # email_action.send_message("5533061@qq.com", 'joinquant record kdata finished', '')
            break
        except Exception as e:
            msg = f'joinquant record kdata:{e}'
            logger.exception(msg)

            # email_action.send_message("5533061@qq.com", 'joinquant record kdata error', msg)
            time.sleep(60)


if __name__ == '__main__':
    init_log('stock_data_runner.log')

    record_block()
    record_money_flow()
    record_dkdata()
    record_wkdata()
    sched.start()

    sched._thread.join()
