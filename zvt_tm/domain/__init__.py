# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it

from zvt.contract.register import register_schema
from zvt.domain.quotes import Stock1dKdata, Stock1wkKdata
from zvt.domain.quotes.trade_day import StockTradeDay

register_schema(providers=['baostock'], db_name='stock_1d_kdata', schema_base=Stock1dKdata)
register_schema(providers=['baostock'], db_name='stock_1wk_kdata', schema_base=Stock1wkKdata)
register_schema(providers=['baostock'], db_name='trade_day', schema_base=StockTradeDay)

