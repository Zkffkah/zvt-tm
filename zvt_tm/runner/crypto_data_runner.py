# -*- coding: utf-8 -*-
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler
from zvt import init_log
from zvt.contract.api import get_entities

from zvt_crypto.domain import *

from zvt_tm.informer.discord_informer import DiscordInformer

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour=8, minute=10)
def run():
    while True:

        try:
            COIN_EXCHANGES = ["binance", "huobipro"]
            Coin.record_data(exchanges=COIN_EXCHANGES)
            items = get_entities(entity_type='coin', provider='ccxt', exchanges=COIN_EXCHANGES)
            entity_ids = [eid for eid in items['entity_id'].to_list() if "USDT" in eid]
            Coin1dKdata.record_data(provider='ccxt', entity_ids= entity_ids, sleeping_time=0.5)

            logger.log('crypto data runner finish')

        except Exception as e:
            logger.exception(e)
            time.sleep(60)


if __name__ == '__main__':
    init_log('crypto_data_runner.log')

    run()

    sched.start()

    sched._thread.join()
