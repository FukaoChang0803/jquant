import logging
from collections import Iterable
import pandas as pd
from Indicator import Atr


def calc_atr(kline_df):
    """
    为输入的kline_df金融时间序列计算atr21和atr14，计算结果直接加到kline_df的atr21列和atr14列中
    :param kline_df: 金融时间序列pd.DataFrame对象
    """
    kline_df['atr21_ewm'] = 0
    kline_df['atr21_ma'] = 0
    if kline_df.shape[0] > 21:
        # 大于21d计算atr21
        kline_df['atr21_ewm'] = Atr.calc_atr_from_pd(kline_df['high'].values,
                                                     kline_df['low'].values,
                                                     kline_df['close'].values,
                                                     time_period=21)
        # 将前面的bfill
        kline_df['atr21_ewm'].fillna(method='bfill', inplace=True)

        # 大于21d计算atr21
        kline_df['atr21_ma'] = Atr.calc_atr_from_ta(kline_df['high'].values,
                                                    kline_df['low'].values,
                                                    kline_df['close'].values,
                                                    time_period=21)
        # 将前面的bfill
        kline_df['atr21_ma'].fillna(method='bfill', inplace=True)

    kline_df['atr14_ewm'] = 0
    kline_df['atr14_ma'] = 0
    if kline_df.shape[0] > 14:
        # 大于14d计算atr14
        kline_df['atr14_ewm'] = Atr.calc_atr_from_pd(kline_df['high'].values,
                                                     kline_df['low'].values,
                                                     kline_df['close'].values,
                                                     time_period=14)
        # 将前面的bfill
        kline_df['atr14_ewm'].fillna(method='bfill', inplace=True)

        # 大于14d计算atr14
        kline_df['atr14_ma'] = Atr.calc_atr_from_ta(kline_df['high'].values,
                                                    kline_df['low'].values,
                                                    kline_df['close'].values,
                                                    time_period=14)
        # 将前面的bfill
        kline_df['atr14_ma'].fillna(method='bfill', inplace=True)

    kline_df['returns'] = kline_df['adjclose'].pct_change()
    kline_df.fillna(0, inplace=True)

    return kline_df
