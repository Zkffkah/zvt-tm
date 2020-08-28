# -*- coding: utf-8 -*-
from zvt.contract import IntervalLevel
from zvt.domain import ReportPeriod

exchangeMap = {
    'binance': 'Binance',
    'huobipro': 'HuobiPro',
    'okex': 'OKEX',
    'ftx': 'ftx'
}

def to_cc_exchange(name):
    return exchangeMap.get(name)
