import pandas as pd
from zvt.factors import Transformer, DataReader, time
from zvt_ccxt import Coin1dKdata, Coin

from zvt_tm.factors.indicators import add_tm_heikin_ashi_features, add_adx_features, add_tm_ema_features, \
    add_ichimoku_features


class TMTransformer(Transformer):
    def __init__(self, ) -> None:
        super().__init__()

    def transform(self, input_df) -> pd.DataFrame:
        self.logger.info('tm transform start')

        start_time = time.time()
        input_df = add_tm_heikin_ashi_features(input_df)
        cost_time = time.time() - start_time
        self.logger.info('tm heikin ashi finished,cost_time:{}'.format(cost_time))

        # input_df = add_adx_features(input_df,
        #                             ohlcv_col={'close': 'H_Close', 'open': 'H_Open', 'high': 'H_High', 'low': 'H_Low',
        #                                        'volume': 'volume'})

        start_time = time.time()
        input_df = add_tm_ema_features(input_df, ohlcv_col={'close': 'H_Close', 'open': 'H_Open', 'high': 'H_High',
                                                            'low': 'H_Low',
                                                            'volume': 'volume'})
        cost_time = time.time() - start_time
        self.logger.info('tm ema finished,cost_time:{}'.format(cost_time))

        start_time = time.time()
        input_df = add_ichimoku_features(input_df, ohlcv_col={'close': 'H_Close', 'open': 'H_Open', 'high': 'H_High',
                                                              'low': 'H_Low',
                                                              'volume': 'volume'})
        cost_time = time.time() - start_time
        self.logger.info('tm ichimoku finished,cost_time:{}'.format(cost_time))

        print('transform out', input_df)
        return input_df


if __name__ == '__main__':
    data_reader1 = DataReader(exchanges=['binance'], codes=['BTC/USDT'], data_schema=Coin1dKdata, entity_schema=Coin)

    t = TMTransformer()

    result_df = t.transform(data_reader1.data_df)
