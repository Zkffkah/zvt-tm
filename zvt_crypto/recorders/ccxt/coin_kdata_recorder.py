# -*- coding: utf-8 -*-
import argparse

from zvt import init_log
from zvt.api import generate_kdata_id, get_kdata_schema
from zvt.contract import IntervalLevel
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.utils.time_utils import to_pd_timestamp
from zvt.utils.time_utils import to_time_str

from zvt_crypto.accounts import CCXTAccount
from zvt_crypto.domain import Coin, CoinKdataCommon


class CoinKdataRecorder(FixedCycleDataRecorder):
    provider = 'ccxt'

    entity_provider = 'ccxt'
    entity_schema = Coin

    # 只是为了把recorder注册到data_schema
    data_schema = CoinKdataCommon

    def __init__(self,
                 exchanges=['binance'],
                 entity_ids=None,
                 codes=None,
                 day_data=False,
                 batch_size=10,
                 force_update=True,
                 sleeping_time=1,
                 default_size=2000,
                 real_time=False,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 level=IntervalLevel.LEVEL_1DAY,
                 kdata_use_begin_time=True,
                 close_hour=None,
                 close_minute=None,
                 one_day_trading_minutes=24 * 60) -> None:
        self.data_schema = get_kdata_schema(entity_type='coin', level=level)
        self.ccxt_trading_level = level

        super().__init__('coin', exchanges, entity_ids, codes, day_data, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data['timestamp'], level=self.level)

    def record(self, entity, start, end, size, timestamps):
        ccxt_exchange = CCXTAccount.get_ccxt_exchange(entity.exchange)

        if ccxt_exchange.has['fetchOHLCV']:
            limit = CCXTAccount.get_kdata_limit(entity.exchange)

            limit = min(size, limit)

            kdata_list = []

            if CCXTAccount.exchange_conf[entity.exchange]['support_since'] and start:
                kdatas = ccxt_exchange.fetch_ohlcv(entity.code,
                                                   timeframe=self.ccxt_trading_level.value,
                                                   since=int(start.timestamp() * 1000))
            else:
                kdatas = ccxt_exchange.fetch_ohlcv(entity.code,
                                                   timeframe=self.ccxt_trading_level.value,
                                                   limit=limit)

            for kdata in kdatas:
                current_timestamp = kdata[0]
                if self.level == IntervalLevel.LEVEL_1DAY:
                    current_timestamp = to_time_str(current_timestamp)

                kdata_json = {
                    'timestamp': to_pd_timestamp(current_timestamp),
                    'open': kdata[1],
                    'high': kdata[2],
                    'low': kdata[3],
                    'close': kdata[4],
                    'volume': kdata[5],
                    'name': entity.name,
                    'provider': 'ccxt',
                    'level': self.level.value
                }
                kdata_list.append(kdata_json)

            return kdata_list
        else:
            self.logger.warning("exchange:{} not support fetchOHLCV".format(entity.exchange))


__all__ = ["CoinKdataRecorder"]

if __name__ == '__main__':
    # CoinKdataRecorder(exchanges=['binance']).run()
    CoinKdataRecorder(exchanges=['huobipro'],entity_ids=['coin_huobipro_1INCH/USDT'], sleeping_time=0.5).run()
    # CoinKdataRecorder(exchanges=['ftx']).run()

    # CoinKdataRecorder(exchanges=['ftx'], codes=['BTC-MOVE-0831']).run()
