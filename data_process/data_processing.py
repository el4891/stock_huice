import pandas as pd
import pandas_ta as ta
from matplotlib import pyplot as plt


class DataProcessing:
    def __init__(self, filename):
        self.filename = filename
        self.dataFrame = pd.read_csv(filename)

        self.__col_processing()

    def cal_strategy(self):
        # self.dataFrame.ta.strategy("All")
        self.dataFrame['SMA'] = ta.sma(self.dataFrame['close'], length=16)
        self.dataFrame['RSI'] = ta.rsi(self.dataFrame['close'], length=16)

    def __col_processing(self):
        self.dataFrame = self.dataFrame.rename(columns={'日期': 'date', '时间': 'time'})
        self.dataFrame['datetime'] = pd.to_datetime(self.dataFrame['date'] + self.dataFrame['time'], format='%Y%m%d%H%M')
        self.dataFrame.set_index('datetime', inplace=True)

    def draw_plot(self):
        ax = self.dataFrame.plot(y=['close', 'open'], kind='line', title='zhexian', marker='o')
        plt.xlabel('时间')
        plt.ylabel('值')
        plt.grid(True)
        plt.legend(title='列名')
        plt.show()
