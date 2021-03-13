# -*- coding: utf-8 -*-
import logging
import operator
from datetime import timedelta
from itertools import accumulate
from typing import List, Union

import pandas as pd
from zvt.contract import IntervalLevel, EntityMixin
from zvt.contract.api import get_entities
from zvt.contract.factor import Accumulator
from zvt.contract.factor import Transformer
from zvt.domain import Stock
from zvt.factors.technical_factor import TechnicalFactor
from zvt.utils.pd_utils import index_df
from zvt.utils.time_utils import now_pd_timestamp

from zvt_tm.factors.tsi_transformer import TSITransformer
from zvt_tm.informer.tradingview_informer import add_list_to_group

logger = logging.getLogger(__name__)

class TSIFactor(TechnicalFactor):
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
        transformer: Transformer = TSITransformer()

        super().__init__(entity_schema, provider, entity_provider, entity_ids, exchanges, codes, the_timestamp,
                         start_timestamp, end_timestamp, columns, filters, order, limit, level, category_field,
                         time_field, computing_window, keep_all_timestamp, fill_method, effective_number, transformer,
                         accumulator, need_persist, dry_run)

    def do_compute(self):
        super().do_compute()
        #下穿
        s = (self.factor_df['tm_tsi_signal'] == 'b')
        self.result_df = s.to_frame(name='score')

def to_tradingview_code(code):
    # 上海
    if code >= '333333':
        return f'SSE:{code}'
    else:
        return f'SZSE:{code}'


if __name__ == '__main__':
    print('start')
    target_date = now_pd_timestamp() - timedelta(1)
    start_date = target_date - timedelta(720)
    factor = TSIFactor(entity_schema=Stock, provider='joinquant', level=IntervalLevel.LEVEL_1DAY,
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
    good_stocks = set(long_result['entity_id'].tolist())
    stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=good_stocks,
                                          return_type='domain')
    codeList = []
    for stock in stocks:
        codeList.append(to_tradingview_code(stock.code))
    add_list_to_group(codeList, group_id=22081672)
    info = [f'{stock.name}({stock.code})' for stock in stocks]
    msg = '选股:' + ' '.join(info) + '\n'
    logger.info(msg)