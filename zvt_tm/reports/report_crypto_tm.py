# -*- coding: utf-8 -*-
import logging
import time
from datetime import timedelta, date

from apscheduler.schedulers.background import BackgroundScheduler
from zvt import init_log
from zvt.factors.target_selector import TargetSelector
from zvt_ccxt import Coin

from zvt_tm.factors.tm_factor import TMFactor
from zvt_tm.informer.discord_informer import DiscordInformer

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job('cron', hour=8, minute=10)
def report_tm():
    while True:
        error_count = 0
        discord_informer = DiscordInformer()

        try:
            # 抓取k线数据
            # Coin1dKdata.record_data()

            target_date = date.today() - timedelta(days=1)
            start_date = target_date - timedelta(60)
            # 计算
            my_selector = TargetSelector(entity_schema=Coin, provider='ccxt',
                                         start_timestamp=start_date, end_timestamp=target_date)
            # add the factors
            tm_factor = TMFactor(entity_schema=Coin, provider='ccxt',
                                 start_timestamp=start_date,
                                 end_timestamp=target_date)

            my_selector.add_filter_factor(tm_factor)

            my_selector.run()

            long_targets = list(map(lambda x: x.replace('_', '.'), filter(lambda k: ('USDT' in k) or ('PERP' in k),
                                                                          my_selector.get_open_long_targets(
                                                                              timestamp=target_date))))

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


if __name__ == '__main__':
    init_log('repot_crypto_tm.log')
    report_tm()

    sched.start()

    sched._thread.join()
