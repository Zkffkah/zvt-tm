# -*- coding: utf-8 -*-
import logging
import time
from datetime import timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from zvt import init_log
from zvt.contract import IntervalLevel
from zvt.contract.api import get_entities
from zvt.domain import Stock, StockTradeDay, Stock1dKdata, StockValuation, Block, BlockStock
from zvt.factors.ma.ma_factor import ImprovedMaFactor
from zvt.factors.money_flow_factor import BlockMoneyFlowFactor
from zvt.factors.target_selector import TargetSelector
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import now_pd_timestamp, to_pd_timestamp
from zvt_ccxt import Coin

from zvt_tm.factors.block_selector import BlockSelector
from zvt_tm.factors.tsi_factor import TSIFactor
from zvt_tm.informer.discord_informer import DiscordInformer
from zvt_tm.informer.tonghuashun_informer import add_to_group

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour=19, minute=0, day_of_week='mon-fri')
def report_tsi():
    while True:
        error_count = 0
        discord_informer = DiscordInformer()

        try:
            # 抓取k线数据
            # StockTradeDay.record_data(provider='baostock', sleeping_time=2)
            # Stock1dKdata.record_data(provider='baostock', sleeping_time=1.5)

            latest_day: StockTradeDay = StockTradeDay.query_data(order=StockTradeDay.timestamp.desc(), limit=1,provider='joinquant',
                                                                 return_type='domain')
            if latest_day:
                target_date = latest_day[0].timestamp
            else:
                target_date = now_pd_timestamp()

            start_date = target_date - timedelta(360)

            # 计算
            my_selector = TargetSelector(entity_schema=Stock, provider='joinquant',
                                         start_timestamp=start_date, end_timestamp=target_date)
            # add the factors
            tsi_factor = TSIFactor(entity_schema=Stock, provider='joinquant',
                                 start_timestamp=start_date,
                                 end_timestamp=target_date)

            my_selector.add_filter_factor(tsi_factor)

            my_selector.run()

            long_targets = my_selector.get_open_long_targets(timestamp=target_date)

            logger.info(long_targets)

            msg = 'no targets'

            # 过滤亏损股
            # check StockValuation data
            pe_date = target_date - timedelta(10)
            if StockValuation.query_data(start_timestamp=pe_date, limit=1, return_type='domain'):
                positive_df = StockValuation.query_data(provider='joinquant', entity_ids=long_targets,
                                                        start_timestamp=pe_date,
                                                        filters=[StockValuation.pe > 0],
                                                        columns=['entity_id'])
                bad_stocks = set(long_targets) - set(positive_df['entity_id'].tolist())
                if bad_stocks:
                    stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=bad_stocks,
                                          return_type='domain')
                    info = [f'{stock.name}({stock.code})' for stock in stocks]
                    msg = '亏损股:' + ' '.join(info) + '\n'

                long_stocks = set(positive_df['entity_id'].tolist())


                if long_stocks:
                    stocks = get_entities(provider='joinquant', entity_schema=Stock, entity_ids=long_stocks,
                                          return_type='domain')
                    for stock in stocks:
                        add_to_group(stock.code, group_id='23')
                    info = [f'{stock.name}({stock.code})' for stock in stocks]
                    msg = msg + '盈利股:' + ' '.join(info) + '\n'

            discord_informer.send_message(f'{target_date} TSI选股结果 {msg}')

            break
        except Exception as e:
            logger.exception('report_tsi error:{}'.format(e))
            time.sleep(60 * 3)
            error_count = error_count + 1
            if error_count == 10:
                discord_informer.send_message(f'report_tsi error',
                                              'report_tsi error:{}'.format(e))

if __name__ == '__main__':
    init_log('repot_crypto_tsi.log')
    report_tsi()

    sched.start()

    sched._thread.join()
