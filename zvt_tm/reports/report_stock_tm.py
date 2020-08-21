# -*- coding: utf-8 -*-
import logging
import time
from datetime import timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from zvt import init_log
from zvt.contract import IntervalLevel
from zvt.domain import Stock, StockTradeDay, Stock1dKdata
from zvt.factors.ma.ma_factor import ImprovedMaFactor
from zvt.factors.target_selector import TargetSelector
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import now_pd_timestamp, to_pd_timestamp
from zvt_ccxt import Coin

from zvt_tm.factors.tm_factor import TMFactor
from zvt_tm.informer.discord_informer import DiscordInformer

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour=19, minute=0, day_of_week='mon-fri')
def report_tm():
    while True:
        error_count = 0
        discord_informer = DiscordInformer()

        try:
            # 抓取k线数据
            StockTradeDay.record_data(provider='baostock', sleeping_time=2)
            Stock1dKdata.record_data(provider='baostock', sleeping_time=2)


            target_date = now_pd_timestamp()

            # 计算
            my_selector = TargetSelector(entity_schema=Stock, provider='baostock',
                                         start_timestamp='2020-05-01', end_timestamp=target_date)
            # add the factors
            tm_factor = TMFactor(entity_schema=Stock, provider='baostock',
                                 start_timestamp='2020-05-01',
                                 end_timestamp=target_date)

            my_selector.add_filter_factor(tm_factor)

            my_selector.run()

            long_targets = get_turning_point(my_selector.open_long_df, target_date)

            logger.info(long_targets)

            discord_informer.send_message(f'{target_date} TM选股结果 {long_targets}')

            break
        except Exception as e:
            logger.exception('report_tm error:{}'.format(e))
            time.sleep(60 * 3)
            error_count = error_count + 1
            if error_count == 10:
                discord_informer.send_message(f'report_tm error',
                                              'report_tm error:{}'.format(e))


def get_turning_point(df, timestamp):
    if pd_is_not_null(df):
        if timestamp in df.index:
            df['difference'] = df.groupby('entity_id')['timestamp'].diff().fillna(0)
            df = df[df['difference'] > timedelta(days=1)]
            target_df = df.loc[[to_pd_timestamp(timestamp)], :]
            return target_df['entity_id'].tolist()
    return []


if __name__ == '__main__':
    init_log('repot_crypto_tm.log')
    report_tm()

    sched.start()

    sched._thread.join()
