import pandas as pd
from zvt.domain import Stock1dKdata, Stock
from zvt.factors import Transformer, DataReader, time
from zvt_ccxt import Coin1dKdata, Coin

from zvt_tm.factors.indicators import add_tm_heikin_ashi_features, add_adx_features, add_tm_ema_features, \
    add_ichimoku_features, add_tm_tsi_features


class TSITransformer(Transformer):
    def __init__(self, ) -> None:
        super().__init__()

    def transform(self, input_df) -> pd.DataFrame:
        self.logger.info('tm tsi transform start')

        start_time = time.time()
        input_df = add_tm_tsi_features(input_df)
        cost_time = time.time() - start_time
        self.logger.info('tm tsi finished,cost_time:{}'.format(cost_time))

        print('transform out', input_df)
        return input_df


if __name__ == '__main__':
    data_reader1 = DataReader(provider='joinquant', codes=['600121'], data_schema=Stock1dKdata, entity_schema=Stock)

    t = TSITransformer()

    result_df = t.transform(data_reader1.data_df)
