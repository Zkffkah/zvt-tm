# -*- coding: utf-8 -*-
import logging
import operator
from datetime import timedelta
from itertools import accumulate

from apscheduler.schedulers.background import BackgroundScheduler
from zvt import init_log
from zvt.contract import IntervalLevel
from zvt.contract.api import get_entities
from zvt.utils.pd_utils import index_df
from zvt.utils.time_utils import now_pd_timestamp

from zvt_crypto import Coin
from zvt_tm.factors.tsi_factor import TSIFactor
from zvt_tm.informer.tradingview_informer import add_list_to_group

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


def to_tradingview_code(code, exchange):
    code = code.replace('/', '')
    if exchange == 'binance':
        return f'BINANCE:{code}'
    elif exchange == 'huobipro':
        return f'HUOBI:{code}'


if __name__ == '__main__':
    init_log('repot_crypto_tsi.log')
    print('start')
    target_date = now_pd_timestamp() - timedelta(1)
    start_date = target_date - timedelta(720)
    COIN_EXCHANGES = ["huobipro"]
    items = get_entities(entity_type='coin', provider='ccxt', exchanges=COIN_EXCHANGES)
    entity_ids = [eid for eid in items['entity_id'].to_list() if "USDT" in eid]

    factor = TSIFactor(entity_schema=Coin, entity_ids=entity_ids, provider='ccxt', level=IntervalLevel.LEVEL_1DAY,
                       start_timestamp=start_date, need_persist=False)
    df = factor.result_df
    musts = []
    if len(df.columns) > 1:
        s = df.agg("and", axis="columns")
        s.name = 'score'
        musts.append(s.to_frame(name='score'))
    else:
        df.columns = ['score']
        musts.append(df)

    filter_result = list(accumulate(musts, func=operator.__and__))[-1]
    long_result = df[df.score == True]
    long_result = long_result.reset_index()
    long_result = index_df(long_result)
    long_result = long_result.sort_values(by=['score', 'entity_id'])
    long_result = long_result[long_result.timestamp > target_date - timedelta(8)]
    longdf = factor.factor_df[factor.factor_df['entity_id'].isin(long_result['entity_id'].tolist())]
    good_coins = set(long_result['entity_id'].tolist())
    coins = get_entities(provider='ccxt', entity_schema=Coin, entity_ids=good_coins,
                         return_type='domain')
    codeList = []
    for coin in coins:
        codeList.append(to_tradingview_code(coin.code, coin.exchange))
    info = [f'{coin.name}({coin.code})' for coin in coins]
    msg = '选币:' + ' '.join(info) + '\n'
    logger.info(msg)
    add_list_to_group(codeList, group_id=19580865, entity_type='coin')
    sched.start()

    sched._thread.join()
