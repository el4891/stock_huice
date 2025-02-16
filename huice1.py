import os
import traceback
from concurrent.futures import ProcessPoolExecutor

import backtrader as bt
import pandas as pd

from data_process import data_processing
from data_process.data_processing import DataProcessing


class MyBaseStrategy(bt.Strategy):

    def __init__(self):
        self.macd = bt.indicators.MACD(self.data.close, period_me1=12, period_me2=26, period_signal=9)

        self.rsi = bt.indicators.RSI(self.data.close, period=16, upperband=40, lowerband=60)

        self.sma1 = bt.indicators.SMA(self.data.close, period=2)
        self.sma2 = bt.indicators.SMA(self.data.close, period=32)
        self.sma3 = bt.indicators.SMA(self.data.close, period=256)

        self.adx = bt.indicators.ADX(self.data, period=16)
        # self.boll = bt.indicators.BollingerBands(self.data.close, period=20, devfactor=2)

    def stop(self):
        if self.position:
            self.close()


class MacdStrategy(MyBaseStrategy):
    def next(self):
        if (self.adx < 15):
            # if (self.position.size < 5 and self.rsi < 30):
            #     self.buy()
            # elif (self.position.size > -5  and self.rsi > 70):
            #     self.sell()
            pass
        else:
            if (self.position.size < 5 and self.macd.macd[0] > self.macd.signal[0] and self.sma1 > self.sma2):
                self.buy()
            elif (self.position.size > -5 and self.macd.macd[0] < self.macd.signal[0] and self.sma1 < self.sma2):
                self.sell()


class ButtomStrategy(MyBaseStrategy):
    def __init__(self):
        super().__init__()
        self.buyflag = 0
        self.sellflag = 0

    def next(self):
        if ((self.position.size > 0 and self.data.close[0] < self.position.price * 0.98)
                or (self.position.size < 0 and self.data.close[0] > self.position.price * 1.02)):
            self.close()

        self.buyflag += 1
        self.sellflag += 1
        if -3 < self.position.size < 3:
            if (self.sma1[-10] < self.sma2[-10] < self.sma3[-10] and self.sma1[0] > self.sma2[0]):
                if self.adx < 20 and self.buyflag > 20:
                    self.buyflag = 0
                    self.buy()
                    return

            elif (self.sma1[-10] > self.sma2[-10] > self.sma3[-10] and self.sma1[0] < self.sma2[0]):
                if self.adx < 20 and self.sellflag > 20:
                    self.sellflag = 0
                    self.sell()
                    return

        if self.position.size > 0:
            if (self.sma1[-1] > self.sma2[-1] > self.sma3[-1] and self.sma1[0] < self.sma2[0]):
                self.sell()
                self.sellflag = 0
                return
        elif self.position.size < 0:
            if (self.sma1[-1] < self.sma2[-1] < self.sma3[-1] and self.sma1[0] > self.sma2[0]):
                self.buy()
                self.buyflag = 0
                return


class DutuStrategy(MyBaseStrategy):
    def __init__(self):
        super().__init__()

    def next(self):
        if self.position.size == 0:
            if self.sma1[0] > self.sma2[0] > self.sma3[0] and self.adx > 20:
                self.buy()
            elif self.sma1[0] < self.sma2[0] < self.sma3[0] and self.adx > 20:
                self.sell()
        else:
            if self.position.size > 0 and self.data.close[0] < self.position.price * 0.99:
                self.close()
            elif self.position.size < 0 and self.data.close[0] > self.position.price * 1.01:
                self.close()

            if self.position.size > 0 and self.sma1[0] < self.sma2[0]:
                self.close()
            elif self.position.size < 0 and self.sma1[0] > self.sma2[0]:
                self.close()


class FenduanStrategy(MyBaseStrategy):
    def __init__(self):
        super().__init__()
        self.jiangelv = 0.06

    def next(self):
        match self.position.size:
            case 0:
                if self.sma1[0] < self.sma2[0] < self.sma3[0]:
                    self.buy()
                elif self.sma1[0] > self.sma2[0] > self.sma3[0]:
                    self.sell()
            case 1:
                if self.data.close[0] < self.position.price * (1 - abs(self.position.size) * self.jiangelv):
                    self.buy()
                elif self.data.close[0] > self.position.price * (1 + (5 - abs(self.position.size)) * self.jiangelv):
                    self.sell()
            case 2:
                if self.data.close[0] < self.position.price * (1 - abs(self.position.size) * self.jiangelv):
                    self.buy()
                elif self.data.close[0] > self.position.price * (1 + (5 - abs(self.position.size)) * self.jiangelv):
                    self.sell()
            case 3:
                if self.data.close[0] < self.position.price * (1 - abs(self.position.size) * self.jiangelv):
                    self.buy()
                elif self.data.close[0] > self.position.price * (1 + (5 - abs(self.position.size)) * self.jiangelv):
                    self.sell()
            case 4:
                if self.data.close[0] > self.position.price * (1 + (5 - abs(self.position.size)) * self.jiangelv):
                    self.sell()
            case -1:
                if self.data.close[0] > self.position.price * (1 + abs(self.position.size) * self.jiangelv):
                    self.sell()
                elif self.data.close[0] < self.position.price * (1 - (5 - abs(self.position.size)) * self.jiangelv):
                    self.buy()
            case -2:
                if self.data.close[0] > self.position.price * (1 + abs(self.position.size) * self.jiangelv):
                    self.sell()
                elif self.data.close[0] < self.position.price * (1 - (5 - abs(self.position.size)) * self.jiangelv):
                    self.buy()
            case -3:
                if self.data.close[0] > self.position.price * (1 + abs(self.position.size) * self.jiangelv):
                    self.sell()
                elif self.data.close[0] < self.position.price * (1 - (5 - abs(self.position.size)) * self.jiangelv):
                    self.buy()
            case -4:
                if self.data.close[0] < self.position.price * (1 - (5 - abs(self.position.size)) * self.jiangelv):
                    self.buy()


def line_product(filename):
    line1_list = [2]
    line2_list = [16]
    line3_list = [64]
    line4_list = [128]
    line5_list = [256]
    zhisun_rate_list1 = [0.015]
    zhisun_rate_list2 = [0.3]
    zhisun_rate_list3 = [0.4]
    max_gushu_list = [8]
    zoneCalDaysList = [1]
    parts_list = [3]

    print(filename)
    filepath = 'line/' + filename
    fileOutPath = f'data_out/data_{filename}.csv'
    df = None
    if os.path.exists(fileOutPath):
        df = pd.read_csv(fileOutPath, header=0, low_memory=False)
    else:
        # df = pd.read_csv(filepath, header=1, low_memory=False)
        # df = df.iloc[:-1]

        try:
            d_p = DataProcessing(filepath)
            df = d_p.get_data()
        except Exception as e:
            traceback.print_exc()

        if not os.path.exists('data_out'):
            os.makedirs('data_out')
        df.to_csv(fileOutPath)

    df = data_processing.get_bt_data(df)

    data = bt.feeds.PandasData(dataname=df)
    cerebro = bt.Cerebro()
    cerebro.addstrategy(FenduanStrategy)
    cerebro.adddata(data)
    # Set our desired cash start
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)
    try:
        # Print out the starting conditions
        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

        # Run over everything
        cerebro.run()

        # Print out the final result
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
        print('Final cash Value: %.2f' % cerebro.broker.getcash())
        # Plot the result
        cerebro.plot()

    except Exception as e:
        traceback.print_exc()


def process():
    filelist = None
    for i, j, k in os.walk('line'):
        filelist = k
        print(filelist)

    with ProcessPoolExecutor(max_workers=16) as executor:
        executor.map(line_product, filelist)


if __name__ == '__main__':
    process()
