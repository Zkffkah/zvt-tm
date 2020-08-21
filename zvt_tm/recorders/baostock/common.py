# -*- coding: utf-8 -*-
from zvt.contract import IntervalLevel
from zvt.domain import ReportPeriod


def to_bs_trading_level(trading_level: IntervalLevel):
    if trading_level < IntervalLevel.LEVEL_1HOUR:
        return trading_level.value

    if trading_level == IntervalLevel.LEVEL_1HOUR:
        return '60'
    if trading_level == IntervalLevel.LEVEL_4HOUR:
        return '240'
    if trading_level == IntervalLevel.LEVEL_1DAY:
        return 'd'
    if trading_level == IntervalLevel.LEVEL_1WEEK:
        return 'w'
    if trading_level == IntervalLevel.LEVEL_1MON:
        return 'm'


def to_bs_entity_id(security_item):
    if security_item.entity_type == 'stock' or security_item.entity_type == 'index':
        if security_item.exchange == 'sh':
            return 'sh.{}'.format(security_item.code)
        if security_item.exchange == 'sz':
            return 'sz.{}'.format(security_item.code)
