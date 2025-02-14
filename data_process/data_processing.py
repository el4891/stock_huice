import pandas as pd
import pandas_ta as ta
from matplotlib import pyplot as plt


class DataProcessing:
    def __init__(self, filename):
        self.filename = filename
        self.dataFrame = pd.read_csv(filename, header=1, low_memory=False).iloc[:-1]

        self.__col_processing()
        self.cal_strategy()
        self.draw_plot()

    def cal_strategy(self):
        # self.dataFrame.ta.strategy("All")
        self.dataFrame['SMA2'] = ta.sma(self.dataFrame['close'], length=2)
        self.dataFrame['SMA16'] = ta.sma(self.dataFrame['close'], length=16)
        self.dataFrame['SMA32'] = ta.sma(self.dataFrame['close'], length=32)
        self.dataFrame['SMA64'] = ta.sma(self.dataFrame['close'], length=64)
        self.dataFrame['SMA128'] = ta.sma(self.dataFrame['close'], length=128)
        self.dataFrame['SMA256'] = ta.sma(self.dataFrame['close'], length=256)
        self.dataFrame['SMA512'] = ta.sma(self.dataFrame['close'], length=512)

        self.dataFrame['RSI'] = ta.rsi(self.dataFrame['close'], length=16)

        self.dataFrame = pd.merge(self.dataFrame, ta.macd(self.dataFrame['close']), on='datetime')

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
                columns={'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'})
            self.dataFrame['datetime'] = pd.to_datetime(self.dataFrame['date'].astype(str), format='%Y%m%d')

        self.dataFrame = self.dataFrame.drop(columns='date')
        self.dataFrame.set_index('datetime', inplace=True)

    def draw_plot(self):
        ax = self.dataFrame.plot(y=['close', 'open'], kind='line', title=self.filename, marker='o')
        plt.xlabel('时间')
        plt.ylabel('值')
        plt.grid(True)
        plt.legend(title='列名')
        plt.show()

    def get_data(self):
        return self.dataFrame
