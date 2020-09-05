# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler
from zvt import init_log
from zvt_ccxt.domain import *

from zvt_tm.informer.discord_informer import DiscordInformer

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour=8, minute=10)
def run():
    while True:
        discord_informer = DiscordInformer()

        try:
            COIN_EXCHANGES = ["binance", "huobipro", "ftx"]
            Coin.record_data(exchanges=COIN_EXCHANGES)
            Coin1dKdata.record_data(exchanges=COIN_EXCHANGES)
            discord_informer.send_message('crypto runner finished', '')
            break
        except Exception as e:
            msg = f'crypto runner error:{e}'
            logger.exception(msg)

            discord_informer.send_message('crypto runner error', msg)
            time.sleep(60)


if __name__ == '__main__':
    init_log('crypto_data_runner.log')

    run()

    sched.start()

    sched._thread.join()
