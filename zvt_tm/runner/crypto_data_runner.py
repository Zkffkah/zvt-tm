# -*- coding: utf-8 -*-
import logging
import time

from zvt import init_log
from zvt.contract.api import get_entities, del_data
from zvt_coin import *

from zvt_coin.domain import *
from zvt_tm.informer.discord_informer import DiscordInformer

logger = logging.getLogger(__name__)



if __name__ == '__main__':
    init_log('crypto_data_runner.log')
    try:
        COIN_EXCHANGES = ["binance"]
        del_data(data_schema=Coin, provider='ccxt')
        Coin.record_data(exchanges=COIN_EXCHANGES)
        items = get_entities(entity_type='coin', provider='ccxt', exchanges=COIN_EXCHANGES)
        entity_ids = items[items['entity_id'].str.contains("USDT")]['entity_id'].tolist()
        Coin1dKdata.record_data(provider='ccxt', exchanges=COIN_EXCHANGES, entity_ids=entity_ids, sleeping_time=0.5)

    except Exception as e:
        logger.exception(e)
        time.sleep(60)


