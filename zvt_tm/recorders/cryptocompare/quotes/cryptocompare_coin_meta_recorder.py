import pandas as pd
from zvt.contract.api import df_to_db
from zvt.contract.recorder import Recorder
from zvt_ccxt.domain import Coin
from zvt_ccxt.settings import COIN_EXCHANGES

from zvt_tm.recorders.cryptocompare.api import cryptocompare
from zvt_tm.recorders.cryptocompare.common import to_cc_exchange


class CryptoCompareCoinMetaRecorder(Recorder):
    provider = 'cryptocompare'
    data_schema = Coin

    def __init__(self, batch_size=10, force_update=False, sleeping_time=1, exchanges=COIN_EXCHANGES) -> None:
        super().__init__(batch_size, force_update, sleeping_time)
        self.exchanges = exchanges

    def run(self):
        for exchange_str in self.exchanges:
            try:
                markets = cryptocompare.get_pairs(exchange=to_cc_exchange(exchange_str))

                df = pd.DataFrame()

                for pair in markets['exchanges'][to_cc_exchange(exchange_str)]['pairs']:
                    # No Kdata for dot_no_split
                    if pair == 'DOT_NO_SPLIT':
                        continue
                    for tsym in markets['exchanges'][to_cc_exchange(exchange_str)]['pairs'][pair]['tsyms']:
                        code = '{}/{}'.format(pair, tsym)
                        name = '{}/{}'.format(pair, tsym)

                        security_item = {
                            'id': '{}_{}_{}'.format('coin', exchange_str, code),
                            'entity_id': '{}_{}_{}'.format('coin', exchange_str, code),
                            'exchange': exchange_str,
                            'entity_type': 'coin',
                            'code': code,
                            'name': name
                        }

                        df = df.append(security_item, ignore_index=True)

                    # 存储该交易所的数字货币列表
                if not df.empty:
                    df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)
                self.logger.info("init_markets for {} success".format(exchange_str))
            except Exception as e:
                self.logger.exception(f"init_markets for {exchange_str} failed", e)


__all__ = ["CryptoCompareCoinMetaRecorder"]

if __name__ == '__main__':
    CryptoCompareCoinMetaRecorder(exchanges=['huobipro']).run()
