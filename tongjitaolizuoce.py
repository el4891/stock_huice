import backtrader as bt

from data_process import data_processing


class PairTradingStrategy(bt.Strategy):
    params = (
        ('window', 900),
        ('entry_z', 1.8),
        ('exit_z', 0.5),
    )

    def __init__(self):
        self.stock1 = self.datas[0]
        self.stock2 = self.datas[1]

        self.spread = self.stock1 - self.stock2

        self.spread_mean = bt.indicators.SimpleMovingAverage(self.spread, period=self.params.window)
        self.spread_std = bt.indicators.StandardDeviation(self.spread, period=self.params.window)

        highlow_period = 32
        self.highest = []
        self.highest.append(bt.ind.Highest(self.stock1.high, period=highlow_period))
        self.highest.append(bt.ind.Highest(self.stock2.high, period=highlow_period))

        self.lowest = []
        self.lowest.append(bt.ind.Lowest(self.stock1.low, period=highlow_period))
        self.lowest.append(bt.ind.Lowest(self.stock2.low, period=highlow_period))

    def next(self):
        if self.spread_std[0] == 0:
            z_score = 0
        else:
            z_score = (self.spread[0] - self.spread_mean[0]) / self.spread_std[0]

        # 获取当前时间
        current_time = self.datas[0].datetime.datetime(0)
        current_date = self.datas[0].datetime.date(0)

        if abs(z_score) > self.params.entry_z:
            print(f"当前时间: {current_time}")
            print(f'zscore: {z_score}')

        chicang = False
        for data in self.datas:
            position = self.getposition(data)
            if position.size != 0:
                chicang = True
                break

        if not chicang:
            if z_score > self.params.entry_z:
                print(f'sell 1 buy 2')
                print(f'1 price {self.stock1.close[0]}  2 price {self.stock2.close[0]}')

                self.sell(data=self.stock1, size=1)
                self.buy(data=self.stock2)
            elif z_score < -self.params.entry_z:
                print(f'sell 2 buy 1')
                print(f'1 price {self.stock1.close[0]}  2 price {self.stock2.close[0]}')

                self.buy(data=self.stock1, size=1)
                self.sell(data=self.stock2)
        else:
            if z_score > self.params.entry_z:
                print(f'you can sell 1 buy 2')
            elif z_score < -self.params.entry_z:
                print(f'you can sell 2 buy 1')

            if abs(z_score) < self.params.exit_z:
                print(f"当前时间: {current_time}")
                print(f'zscore: {z_score}')
                print(f'close')
                print(f'1 price {self.stock1.close[0]}  2 price {self.stock2.close[0]}')

                self.close(data=self.stock1)
                self.close(data=self.stock2)

            else:
                for i in range(2):
                    data = self.datas[i]
                    position = self.getposition(data)

                    if position.size > 0 and (data.close[0] < self.lowest[i][-1]):
                        self.buy(data=data)
                        print(f'buy {i + 1}')
                        print(f'1 price {self.stock1.close[0]}  2 price {self.stock2.close[0]}')
                    elif position.size < 0 and (data.close[0] > self.highest[i][-1]):
                        self.sell(data=data)
                        print(f'sell {i + 1}')
                        print(f'1 price {self.stock1.close[0]}  2 price {self.stock2.close[0]}')

                    if position.size > 0 and (data.close[0] > self.highest[i][-1]):
                        self.sell(data=data)
                        print(f'sell {i + 1}')
                        print(f'1 price {self.stock1.close[0]}  2 price {self.stock2.close[0]}')
                    elif position.size < 0 and (data.close[0] < self.lowest[i][-1]):
                        self.buy(data=data)
                        print(f'buy {i + 1}')
                        print(f'1 price {self.stock1.close[0]}  2 price {self.stock2.close[0]}')


# 回测设置
if __name__ == '__main__':

    futures_list = [['RB2505', 'HC2505'], ['C2505', 'CS2505'], ['FG2505', 'SA2505']]

    for item in futures_list:
        print('-----------------------------------------------------------')
        print(item)
        # df1 = data_processing.sina_daily_data('RB0')
        # df2 = data_processing.sina_daily_data('HC0')

        df1 = data_processing.sina_minute_data(item[0], 60)
        df2 = data_processing.sina_minute_data(item[1], 60)

        cerebro = bt.Cerebro()

        common_index = df1.index.intersection(df2.index)

        df1 = df1.loc[common_index]
        df2 = df2.loc[common_index]

        data1 = bt.feeds.PandasData(dataname=df1)
        data2 = bt.feeds.PandasData(dataname=df2)
        cerebro.adddata(data1)
        cerebro.adddata(data2)

        # 添加策略
        cerebro.addstrategy(PairTradingStrategy)

        cerebro.broker.set_cash(100000)
        cerebro.broker.setcommission(commission=0.001)

        # 运行回测
        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio')
        results = cerebro.run()
        strat = results[0]
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
        print(item)

        print(strat.analyzers.sharpe_ratio.get_analysis())
        print('-----------------------------------------------------------')
        # 绘制结果
        #cerebro.plot()
