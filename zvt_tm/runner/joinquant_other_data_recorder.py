# -*- coding: utf-8 -*-
import logging
import time
from operator import or_

from zvt import init_log
from zvt.contract.api import get_entities
from zvt.domain import Fund, FundStock, Stock1wkHfqKdata, StockValuation, Stock, HkHolder, IndexMoneyFlow, IndexStock, \
    Index
from zvt.utils import now_pd_timestamp

logger = logging.getLogger(__name__)


def record_fund():
    while True:

        try:
            # 基金和基金持仓数据
            Fund.record_data(provider='joinquant', sleeping_time=1, day_data=True)
            entities = Fund.query_data(
                return_type='domain',
                provider='joinquant',
                filters=[Fund.underlying_asset_type.in_(('股票型', '混合型')), Fund.end_date.is_(None)])
            entity_ids = [element.entity_id for element in entities]
            FundStock.record_data(provider='joinquant', entity_ids=entity_ids[3875:], sleeping_time=0)
            # FundStock.record_data(provider='joinquant', entity_ids=entity_ids, sleeping_time=0)

            break
        except Exception as e:
            msg = f'joinquant record fund error:{e}'
            logger.exception(msg)

            time.sleep(60)


def record_hk_holder():
    while True:
        try:
            # IndexMoneyFlow.record_data(provider='joinquant', sleeping_time=0)
            HkHolder.record_data(provider='joinquant', sleeping_time=0)
            break
        except Exception as e:
            msg = f'joinquant record fund error:{e}'
            logger.exception(msg)

            time.sleep(60)


def record_valuation():
    while True:
        try:
            items = get_entities(entity_type='stock', provider='joinquant',
                                 filters=[or_(Stock.end_date.is_(None), Stock.end_date > now_pd_timestamp())])
            entity_ids = items['entity_id'].to_list()
            # StockValuation.record_data(provider='joinquant', entity_ids=entity_ids[400], sleeping_time=0, day_data=True)
            StockValuation.record_data(provider='joinquant', entity_ids=entity_ids, sleeping_time=0, day_data=True)

            break
        except Exception as e:
            msg = f'joinquant record valuation error:{e}'
            logger.exception(msg)

            time.sleep(60)


if __name__ == '__main__':
    init_log('joinquant_fund_runner.log')

    # record_fund()
    # record_hk_holder()
    # record_valuation()
