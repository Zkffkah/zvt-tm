# -*- coding: utf-8 -*-

import baostock as bs
import pandas as pd

from zvt.api import get_kdata, AdjustType
from zvt.api.quote import generate_kdata_id, get_kdata_schema, StockKdataCommon, Stock
from zvt.contract import IntervalLevel
from zvt.contract.api import df_to_db
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import register_schema, declarative_base

from zvt_tm.domain import Stock1dKdata
from zvt_tm.recorders.baostock.common import to_bs_trading_level, to_bs_entity_id
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601


class BaoStockChinaStockKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'eastmoney'
    entity_schema = Stock

    # 数据来自bs
    provider = 'baostock'

    # 只是为了把recorder注册到data_schema
    data_schema = StockKdataCommon

    def __init__(self,
                 exchanges=['sh', 'sz'],
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
                 level=IntervalLevel.LEVEL_1WEEK,
                 kdata_use_begin_time=False,
                 close_hour=15,
                 close_minute=0,
                 one_day_trading_minutes=4 * 60,
                 adjust_type=AdjustType.qfq) -> None:
        level = IntervalLevel(level)
        adjust_type = AdjustType(adjust_type)
        self.data_schema = get_kdata_schema(entity_type='stock', level=level, adjust_type=adjust_type)
        self.bs_trading_level = to_bs_trading_level(level)

        super().__init__('stock', exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)
        self.adjust_type = adjust_type

        print("尝试登陆baostock")
        #####login#####
        lg = bs.login(user_id="anonymous", password="123456")
        if (lg.error_code == '0'):
            print("登陆成功")
        else:
            print("登录失败")

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data['timestamp'], level=self.level)

    def on_finish(self):
        super().on_finish()
        bs.logout()

    def record(self, entity, start, end, size, timestamps):
        if self.adjust_type == AdjustType.hfq:
            adflag = '1'
        else:
            adflag = '2'

        if not self.end_timestamp:
            data = bs.query_history_k_data(to_bs_entity_id(entity),
                                           "date,code,open,high,low,close,volume,amount",
                                           start_date=to_time_str(start),
                                           frequency=self.bs_trading_level, adjustflag=adflag)
        else:
            end_timestamp = to_time_str(self.end_timestamp)
            data = bs.query_history_k_data(to_bs_entity_id(entity),
                                         "date,code,open,high,low,close,volume,amount",
                                         start_date = to_time_str(start),
                                         end_date = end_timestamp,
                                         frequency=self.bs_trading_level, adjustflag=adflag)
        df = data.get_data()
        if pd_is_not_null(df):
            df['name'] = entity.name
            df.rename(columns={'amount': 'turnover', 'date': 'timestamp'}, inplace=True)

            df['entity_id'] = entity.id
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['provider'] = 'baostock'
            df['level'] = self.level.value
            df['code'] = entity.code

            def generate_kdata_id(se):
                if self.level >= IntervalLevel.LEVEL_1DAY:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_DAY))
                else:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

            df['id'] = df[['entity_id', 'timestamp']].apply(generate_kdata_id, axis=1)

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

        return None


__all__ = ['BaoStockChinaStockKdataRecorder']

if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--level', help='trading level', default='1d', choices=[item.value for item in IntervalLevel])
    # parser.add_argument('--codes', help='codes', default=['000001'], nargs='+')
    #
    # args = parser.parse_args()
    #
    # level = IntervalLevel(args.level)
    # codes = args.codes

    # init_log('baostock_china_stock_{}_kdata.log'.format(args.level))
    # BaoStockChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1DAY, sleeping_time=0, codes=['000001'], real_time=False,
    #                           adjust_type=AdjustType.qfq).run()

    Stock1dKdata.record_data(provider='baostock', sleeping_time=1)

    # print(get_kdata(entity_id='stock_sz_000001', limit=10, order=Stock1dHfqKdata.timestamp.desc(),
    #                 adjust_type=AdjustType.hfq))
