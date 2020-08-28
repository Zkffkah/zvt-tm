# -*- coding: utf-8 -*-
from zvt import init_log
from zvt.api import AdjustType
from zvt.api.quote import generate_kdata_id, get_kdata_schema
from zvt.contract import IntervalLevel
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.utils.time_utils import to_time_str, to_pd_timestamp
from zvt_ccxt import Coin, Coin1dKdata, CoinKdataCommon
import zvt_tm.domain as domain
import pandas as pd

from zvt_tm.recorders.cryptocompare.api import cryptocompare
from zvt_tm.recorders.cryptocompare.common import to_cc_exchange


class CryptoCompareCoinKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'cryptocompare'
    entity_schema = Coin


    # 数据来自bs
    provider = 'cryptocompare'

    # 只是为了把recorder注册到data_schema
    data_schema = CoinKdataCommon

    def __init__(self,
                 exchanges=['huobipro', 'binance'],
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=True,
                 sleeping_time=0,
                 default_size=2000,
                 real_time=False,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 level=IntervalLevel.LEVEL_1DAY,
                 kdata_use_begin_time=False,
                 close_hour=15,
                 close_minute=0,
                 one_day_trading_minutes=4 * 60,
                 adjust_type=AdjustType.qfq) -> None:
        level = IntervalLevel(level)
        adjust_type = AdjustType(adjust_type)
        self.data_schema = get_kdata_schema(entity_type='coin', level=level, adjust_type=adjust_type)

        super().__init__('coin', exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)
        self.adjust_type = adjust_type

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data['timestamp'], level=self.level)

    def record(self, entity, start, end, size, timestamps):
        kdatas = cryptocompare.get_historical_price_day(entity.code.split('/')[0], entity.code.split('/')[1],
                                                        limit=min(size, 2000), exchange=to_cc_exchange(entity.exchange))
        kdatas = [x for x in kdatas if (x['open'] != 0 and x['close'] != 0 and x['high'] !=0)]

        # open有误差
        kdata_list = []
        for kdata in kdatas:
            kdata_json = {
                'timestamp':  to_pd_timestamp(float(kdata['time'])),
                'open': kdata['open'],
                'high': kdata['high'],
                'low': kdata['low'],
                'close': kdata['close'],
                'volume': kdata['volumefrom'],
                'name': entity.name,
                'provider': 'cryptocompare',
                'level': self.level.value
            }
            kdata_list.append(kdata_json)

        return kdata_list


__all__ = ['CryptoCompareCoinKdataRecorder']

if __name__ == '__main__':

    CryptoCompareCoinKdataRecorder(level=IntervalLevel.LEVEL_1DAY, sleeping_time=1,exchanges=['huobipro']).run()

    # Coin1dKdata.record_data(provider='cryptocompare', exchanges=['huobipro'], sleeping_time=1)
