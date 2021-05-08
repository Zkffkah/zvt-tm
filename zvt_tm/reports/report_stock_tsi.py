# -*- coding: utf-8 -*-
import logging
import operator
from datetime import timedelta
from itertools import accumulate

from zvt.api import get_top_performance_entities, get_recent_report_date
from zvt.contract import IntervalLevel
from zvt.contract.api import get_entities, get_entity_ids
from zvt.domain import Stock, StockValuation, FinanceFactor, Stock1dHfqKdata, FundStock
from zvt.utils.pd_utils import index_df
from zvt.utils.time_utils import now_pd_timestamp, next_date

from zvt_tm.factors.tsi_factor import TSIFactor
from zvt_tm.informer.tradingview_informer import add_list_to_group

logger = logging.getLogger(__name__)



def to_tradingview_code(code):
    # 上海
    if code >= '333333':
        return f'SSE:{code}'
    else:
        return f'SZSE:{code}'


if __name__ == '__main__':
    print('start')

    latest_day: Stock1dHfqKdata = Stock1dHfqKdata.query_data(order=Stock1dHfqKdata.timestamp.desc(), limit=1,
                                                             return_type='domain')
    current_timestamp = latest_day[0].timestamp

    # 基金持股大于3%
    report_date = get_recent_report_date(current_timestamp, 1)
    fund_cap_df = FundStock.query_data(filters=[FundStock.report_date >= report_date, FundStock.timestamp <= current_timestamp, FundStock.proportion >= 0.03],
                                       columns=['stock_id', 'market_cap', 'proportion'])
    top_holding_stocks = fund_cap_df['stock_id'].tolist()

    # 至少上市一年
    filters = None
    pre_year = next_date(current_timestamp, -365)
    stocks = get_entity_ids(provider='joinquant', entity_schema=Stock, filters=[Stock.timestamp <= pre_year])
    filters = [Stock1dHfqKdata.entity_id.in_(stocks)]

    # 任一rps 大于90
    top_rps_stocks = []
    periods = [50, 120, 250]
    for period in periods:
        start = next_date(current_timestamp, -period)
        updf, _ = get_top_performance_entities(start_timestamp=start, filters=filters, pct=1, show_name=True)
        top_rps_stocks.extend(updf.iloc[:600].index.tolist())


    top_stocks = list(set(top_holding_stocks) & set(top_rps_stocks))

    target_date = now_pd_timestamp() - timedelta(1)
    start_date = target_date - timedelta(420)
    factor = TSIFactor(entity_schema=Stock, provider='joinquant', level=IntervalLevel.LEVEL_1DAY,entity_ids=top_stocks,
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
    long_result = long_result[long_result.timestamp > target_date - timedelta(7)]
    longdf = factor.factor_df[factor.factor_df['entity_id'].isin(long_result['entity_id'].tolist())]
    good_stocks = set(long_result['entity_id'].tolist())

    # 过滤亏损股
    # check StockValuation data
    # pe_date = target_date - timedelta(10)
    # if StockValuation.query_data(start_timestamp=pe_date, limit=1, return_type='domain'):
    #     positive_df = StockValuation.query_data(provider='joinquant', entity_ids=good_stocks,
    #                                             start_timestamp=pe_date,
    #                                             filters=[StockValuation.pe > 0],
    #                                             columns=['entity_id'])
    #
    #     good_stocks = set(positive_df['entity_id'].tolist())
    # 财务数据
    # roe_date = target_date - timedelta(130)
    # if FinanceFactor.query_data(start_timestamp=roe_date, limit=1, return_type='domain'):
    #     # 核心资产=(高ROE 高现金流 高股息 低应收 低资本开支 低财务杠杆 有增长)
    #     # 高roe 高现金流 低财务杠杆 有增长
    #     positive_df = FinanceFactor.query_data(entity_ids=good_stocks, start_timestamp=roe_date,
    #                                            filters=[FinanceFactor.roe >= 0.02,
    #                                                     # FinanceFactor.report_period == 'year',
    #                                                     # 营业总收入同比增长
    #                                                     FinanceFactor.op_income_growth_yoy >= 0.05,
    #                                                     # 归属净利润同比增长
    #                                                     FinanceFactor.net_profit_growth_yoy >= 0.05,
    #                                                     # 经营净现金流 / 营业收入
    #                                                     FinanceFactor.op_net_cash_flow_per_op_income >= 0.1,
    #                                                     # 销售净现金流/营业收入
    #                                                     FinanceFactor.sales_net_cash_flow_per_op_income >= 0.3,
    #                                                     # 流动比率
    #                                                     FinanceFactor.current_ratio >= 1,
    #                                                     # 资产负债率
    #                                                     FinanceFactor.debt_asset_ratio <= 0.5
    #                                                     ],
    #                                            columns=['entity_id'])
    #     good_stocks = set(positive_df['entity_id'].tolist())

    stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=list(good_stocks),
                          return_type='domain')
    codeList = []
    for stock in stocks:
        codeList.append(to_tradingview_code(stock.code))
    add_list_to_group(codeList, group_id=22081672)
    info = [f'{stock.name}({stock.code})' for stock in stocks]
    msg = '选股:' + ' '.join(info) + '\n'
    logger.info(msg)
