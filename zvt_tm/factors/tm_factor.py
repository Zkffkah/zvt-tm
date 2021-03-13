# -*- coding: utf-8 -*-
from typing import List, Union

import pandas as pd
from zvt.contract import IntervalLevel, EntityMixin
from zvt.domain import Stock
from zvt.contract.factor import Accumulator
from zvt.contract.factor import Transformer
from zvt.factors.technical_factor import TechnicalFactor
from zvt.utils.time_utils import now_pd_timestamp
from zvt_crypto import Coin

from zvt_tm.factors.tm_transformer import TMTransformer


class TMFactor(TechnicalFactor):
    def __init__(self, entity_schema: EntityMixin = Stock, provider: str = None, entity_provider: str = None,
                 entity_ids: List[str] = None, exchanges: List[str] = None, codes: List[str] = None,
                 the_timestamp: Union[str, pd.Timestamp] = None, start_timestamp: Union[str, pd.Timestamp] = None,
                 end_timestamp: Union[str, pd.Timestamp] = None,
                 columns: List = ['id', 'entity_id', 'timestamp', 'level', 'open', 'close', 'high', 'low', 'volume',
                                  'turnover'],
                 filters: List = None, order: object = None, limit: int = None,
                 level: Union[str, IntervalLevel] = IntervalLevel.LEVEL_1DAY, category_field: str = 'entity_id',
                 time_field: str = 'timestamp', computing_window: int = None, keep_all_timestamp: bool = False,
                 fill_method: str = 'ffill', effective_number: int = None,
                 accumulator: Accumulator = None, need_persist: bool = False, dry_run: bool = False,
                 ) -> None:
        transformer: Transformer = TMTransformer()

        super().__init__(entity_schema, provider, entity_provider, entity_ids, exchanges, codes, the_timestamp,
                         start_timestamp, end_timestamp, columns, filters, order, limit, level, category_field,
                         time_field, computing_window, keep_all_timestamp, fill_method, effective_number, transformer,
                         accumulator, need_persist, dry_run)

    def do_compute(self):
        super().do_compute()
        s = (self.factor_df['tm_signal'] == 'b')
        self.result_df = s.to_frame(name='score')


if __name__ == '__main__':
    print('start')

    factor = TMFactor(exchanges=['binance'], codes=['BTC/USDT'], start_timestamp='2020-01-01',
                      entity_schema=Coin, provider='ccxt', level=IntervalLevel.LEVEL_1DAY,
                      end_timestamp=now_pd_timestamp(), need_persist=False,
                      )
    print(factor.result_df)
