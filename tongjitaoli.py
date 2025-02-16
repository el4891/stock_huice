import backtrader as bt

from data_process import data_processing


class PairTradingStrategy(bt.Strategy):
    params = (
        ('window', 256),  # 用于计算均值和标准差的窗口期
        ('entry_z', 2.8),  # 开仓阈值（标准差倍数）
        ('exit_z', 1),  # 平仓阈值（标准差倍数）
    )

    def __init__(self):
        # 获取两只股票的数据
        self.stock1 = self.datas[0]
        self.stock2 = self.datas[1]

        # 计算价差
        self.spread = self.stock1 - self.stock2

        # 初始化均值和标准差
        self.spread_mean = bt.indicators.SimpleMovingAverage(self.spread, period=self.params.window)
        self.spread_std = bt.indicators.StandardDeviation(self.spread, period=self.params.window)

    def next(self):
        # 计算当前价差的 Z 分数
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

        # 如果没有持仓
        if not chicang:
            # 如果价差偏离均值超过开仓阈值，开仓
            if z_score > self.params.entry_z:
                print(f'sell 1 buy 2')
                self.sell(data=self.stock1, size=1)  # 卖出股票1
                self.buy(data=self.stock2)  # 买入股票2
            elif z_score < -self.params.entry_z:
                print(f'sell 2 buy 1')
                self.buy(data=self.stock1, size=1)  # 买入股票1
                self.sell(data=self.stock2)  # 卖出股票2
        # 如果已经持仓
        else:
            if z_score > self.params.entry_z:
                print(f'you can sell 1 buy 2')
            elif z_score < -self.params.entry_z:
                print(f'you can sell 2 buy 1')

            # 如果价差回归均值，平仓
            if abs(z_score) < self.params.exit_z:
                self.close(data=self.stock1)
                self.close(data=self.stock2)

            # for data in self.datas:
            #     position = self.getposition(data)
            #
            #     if position.size > 0 and (data.close[0] < position.price * 0.99 or data.close[0] > position.price * 1.02):
            #         self.sell(data=data)
            #     elif position.size < 0 and (data.close[0] > position.price * 1.01 or data.close[0] < position.price * 0.98):
            #         self.buy(data=data)


# 回测设置
if __name__ == '__main__':

    futures_list = [['RB2505', 'HC2505'], ['C2505', 'CS2505'], ['FG2505', 'SA2505']]

    for item in futures_list:
        print('-----------------------------------------------------------')
        print(item)
        # df1 = data_processing.sina_daily_data('RB0')
        # df2 = data_processing.sina_daily_data('HC0')

        df1 = data_processing.sina_minute_data(item[0], 30)
        df2 = data_processing.sina_minute_data(item[1], 30)

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

        # 设置初始资金
        cerebro.broker.set_cash(100000)
        cerebro.broker.setcommission(commission=0.001)

        # 运行回测
        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
        cerebro.run()
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
        print(item)
        print('-----------------------------------------------------------')
        # 绘制结果
        # cerebro.plot()
