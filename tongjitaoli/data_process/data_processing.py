import os

import akshare as ak
import numpy
import pandas as pd


def custom_fill(series):
    # 将 0 替换为 NaN，然后后向填充非零值
    return series.replace(0, numpy.nan).bfill()


def data_resample(df1, df2, factor1=1, factor2=1):
    # 1. 获取索引的并集
    combined_index = df1.index.union(df2.index)

    # 重新索引，填充缺失值为 后值
    df1_new = df1.reindex(combined_index).apply(custom_fill)
    df2_new = df2.reindex(combined_index).apply(custom_fill)

    # fileOutPath = f'data_sina/data_{numpy.random.randint(1, 100, 1)}.csv'
    # df1_new.to_csv(fileOutPath)
    #
    # fileOutPath = f'data_sina/data_{numpy.random.randint(1, 100, 1)}.csv'
    # df2_new.to_csv(fileOutPath)

    if factor1 > 1:
        df1_new['open'] = df1_new['open'] * factor1
        df1_new['close'] = df1_new['close'] * factor1
        df1_new['high'] = df1_new['high'] * factor1
        df1_new['low'] = df1_new['low'] * factor1

    if factor2 > 1:
        df2_new['open'] = df2_new['open'] * factor2
        df2_new['close'] = df2_new['close'] * factor2
        df2_new['high'] = df2_new['high'] * factor2
        df2_new['low'] = df2_new['low'] * factor2

    return df1_new, df2_new


def get_bt_data(df_in):
    df = df_in[['datetime', 'open', 'high', 'low', 'close', 'volume']].copy()
    df['openinterest'] = 0
    df['datetime'] = pd.to_datetime(df['datetime'].astype(str))
    df.set_index('datetime', inplace=True)
    return df


def sina_minute_data(symbol, period):
    fileOutPath = f'data_sina/data_{symbol}_{period}.csv'
    if os.path.exists(fileOutPath):
        df = pd.read_csv(fileOutPath, header=0, low_memory=False)
        df['datetime'] = pd.to_datetime(df['datetime'].astype(str))
        df.set_index('datetime', inplace=True)
    else:
        df = ak.futures_zh_minute_sina(symbol=symbol, period=period)
        df = get_bt_data(df)
        if not os.path.exists('data_sina'):
            os.makedirs('data_sina')
        df.to_csv(fileOutPath)

    return df


def sina_daily_data(symbol):
    fileOutPath = f'data_sina/data_{symbol}_daily.csv'
    if os.path.exists(fileOutPath):
        df = pd.read_csv(fileOutPath, header=0, low_memory=False)
        df['datetime'] = pd.to_datetime(df['datetime'].astype(str))
        df.set_index('datetime', inplace=True)
    else:
        df = ak.futures_zh_daily_sina(symbol=symbol)
        df = df.rename(columns={'date': 'datetime'})
        df = get_bt_data(df)

        if not os.path.exists('data_sina'):
            os.makedirs('data_sina')
        df.to_csv(fileOutPath)

    return df


class DataProcessing:
    def __init__(self, filename):
        self.filename = filename
        self.dataFrame = None

    def __col_processing(self):
        if '时间' in self.dataFrame.columns:
            self.dataFrame = self.dataFrame.rename(
                columns={'日期': 'date', '时间': 'time', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close',
                         '成交量': 'volume'})

            self.dataFrame['date'] = self.dataFrame['date'].astype(int) * 10000
            self.dataFrame['date'] = self.dataFrame['date'] + self.dataFrame['time'].astype(int)
            self.dataFrame['datetime'] = pd.to_datetime(self.dataFrame['date'].astype(str),
                                                        format='%Y%m%d%H%M')
        else:
            self.dataFrame = self.dataFrame.rename(
                columns={'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close',
                         '成交量': 'volume'})
            self.dataFrame['datetime'] = pd.to_datetime(self.dataFrame['date'].astype(str), format='%Y%m%d')

        self.dataFrame = self.dataFrame.drop(columns='date')
        self.dataFrame.set_index('datetime', inplace=True)

    def get_data(self):
        self.dataFrame = pd.read_csv(self.filename, header=1, low_memory=False).iloc[:-1]

        self.__col_processing()

        return self.dataFrame
