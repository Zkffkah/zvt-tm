# -*- coding: utf-8 -*-
from sqlalchemy import Column, DateTime
from sqlalchemy.ext.declarative import declarative_base

from zvt.contract import TradableEntity
from zvt.contract.register import register_schema, register_entity

CoinMetaBase = declarative_base()

@register_entity(entity_type='coin')
class Coin(TradableEntity, CoinMetaBase):
    __tablename__ = 'coin'

register_schema(providers=['ccxt'], db_name='coin', schema_base=CoinMetaBase)

# the __all__ is generated
__all__ = ['Coin']