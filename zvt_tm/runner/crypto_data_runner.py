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
            COIN_EXCHANGES = ["binance", "huobipro"]
            COIN_BASE = ["BTC", "ETH", "XRP", "BCH", "EOS", "LTC", "XLM", "ADA", "IOTA", "TRX", "NEO", "DASH", "XMR",
                         "BNB", "ETC", "QTUM", "ONT"]
            COIN_PAIRS = [("{}/{}".format(item, "USDT")) for item in COIN_BASE] + \
                         [("{}/{}".format(item, "USD")) for item in COIN_BASE]

            Coin.record_data()
            Coin1dKdata.record_data()
            # CoinTickKdata.record_data()
            # CoinKdataRecorder(exchanges=COIN_EXCHANGES, codes=COIN_PAIRS, start_timestamp='2013-08-17',
            #                   level=IntervalLevel.LEVEL_1DAY, real_time=True).run()

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
