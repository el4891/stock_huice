import argparse
import ast
import configparser
import datetime
import os
import shutil
import time

import backtrader as bt
import colorama
import numpy as np
import statsmodels.api as sm

from data_process import data_processing


class context:
    xiangguanxing = 1
    zongchangdu = 0
    window = 0
    use_daily = False


class CustomCorrelation(bt.Indicator):
    # 1 ≥ Corr > 0.7 强正相关
    # 0.7 ≥ Corr > 0.3 中度正相关
    # 0.3 ≥ Corr > -0.3 弱相关或无显著关系
    # -0.3 ≥ Corr > -0.7 中度负相关
    # -0.7 ≥ Corr ≥ -1 强负相关

    lines = ('corr',)
    params = (('period', 600),)

    def __init__(self):
        self.data0_returns = bt.ind.PercentChange(self.data0.close, period=1)
        self.data1_returns = bt.ind.PercentChange(self.data1.close, period=1)
        self.period = self.params.period

    def next(self):
        try:
            d0 = np.array(self.data0_returns.get(size=self.period))
            d1 = np.array(self.data1_returns.get(size=self.period))

            # 计算相关系数
            if len(d0) >= 2 and len(d1) >= 2:  # 确保有足够数据点
                corr = np.corrcoef(d0, d1)[0, 1]
                self.lines.corr[0] = corr
            else:
                self.lines.corr[0] = np.nan
        except Exception as e:
            # print(e)
            pass

class Spread(bt.Indicator):
    lines = ('spread',)
    plotlines = dict(
        spread=dict(color='purple', linestyle='-', linewidth=2.0, _name='Spread')
    )

    def __init__(self):
        # 使用 data0 和 data1 的收盘价，但指标内部拿数据要通过 self.datas
        self.lines.spread = self.datas[0].close - (context.xiangguanxing * self.datas[1].close)

class PairTradingStrategy(bt.Strategy):
    params = (
        ('window', 800),
        ('entry_z', 1.6),
        ('exit_z', 0.5),
    )

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        if context.window > 0:
            self.params.window = context.window
        else:
            self.params.window = int(config['tongjitaoli']['window'])
        self.params.entry_z = float(config['tongjitaoli']['entry'])
        self.params.exit_z = float(config['tongjitaoli']['exit'])

        self.stock1 = self.datas[0]
        self.stock2 = self.datas[1]

        self.spread = self.stock1 - (context.xiangguanxing * self.stock2)
        self.spread_ind = Spread(self.stock1, self.stock2)  # 传入两个数据

        self.spread_mean = bt.indicators.SimpleMovingAverage(self.spread, period=self.params.window)
        self.spread_std = bt.indicators.StandardDeviation(self.spread, period=self.params.window)

        self.orders_buy = []
        self.orders_sell = []

        self.hedge_ratio1 = None
        self.hedge_ratio2 = None

        self.corr_long = CustomCorrelation(self.stock1, self.stock2, period=self.params.window)
        self.corr_short = CustomCorrelation(self.stock1, self.stock2, period=int(self.params.window / 10))

        self.max_zscore_abs = 1.5
        self.second_zscore_abs = 1.5
        self.third_zscore_abs = 1.5
        self.fourth_zscore_abs = 1.5
        self.fifth_zscore_abs = 1.5

    def next(self):
        need_print = False
        if len(self) > context.zongchangdu - 2:
            need_print = True

        self.hedge_ratio1 = self.cal_hedge_ratio(self.stock1.close, self.stock2.close)
        self.hedge_ratio2 = self.cal_hedge_ratio(self.stock2.close, self.stock1.close)

        if self.spread_std[0] == 0:
            z_score = 0
        else:
            z_score = (self.spread[0] - self.spread_mean[0]) / self.spread_std[0]

        # 获取当前时间
        current_time = self.datas[0].datetime.datetime(0)

        chicang = False
        for data in self.datas:
            position = self.getposition(data)
            if position.size != 0:
                chicang = True
                break

        if not chicang:
            if z_score > self.params.entry_z:
                self.sell(data=self.stock1, size=1)
                self.buy(data=self.stock2)
            elif z_score < -self.params.entry_z:
                self.buy(data=self.stock1, size=1)
                self.sell(data=self.stock2)
        else:
            if abs(z_score) < self.params.exit_z:
                self.close(data=self.stock1)
                self.close(data=self.stock2)

        if abs(z_score) > self.max_zscore_abs:
            self.fifth_zscore_abs = self.fourth_zscore_abs
            self.fourth_zscore_abs = self.third_zscore_abs
            self.third_zscore_abs = self.second_zscore_abs
            self.second_zscore_abs = self.max_zscore_abs
            self.max_zscore_abs = abs(z_score)

        if need_print:
            print(f"当前时间: {current_time}")
            # print(f'hedge ratio:{self.hedge_ratio1:.2f} , {self.hedge_ratio2:.2f}')
            # 当指标有足够数据时输出相关性
            if len(self.corr_long) > 0 and len(self.corr_short) > 0:
                current_corr_long = self.corr_long[0]
                current_corr_short = self.corr_short[0]
                price_ratio = self.stock1.close[0] / self.stock2.close[0]
                if price_ratio < 1:
                    price_ratio = 1 / price_ratio
                print(f'Correlation: long:{current_corr_long:.2f} , short:{current_corr_short:.2f} ---- price ratio:{price_ratio:.3f}')

            print(
                f'max zscore: {self.max_zscore_abs:.2f}--{self.second_zscore_abs:.2f}--{self.third_zscore_abs:.2f}--{self.fourth_zscore_abs:.2f}--{self.fifth_zscore_abs:.2f}')
            if abs(z_score) > self.third_zscore_abs or abs(z_score) > 2.5:
                print(colorama.Fore.RED + colorama.Back.WHITE + f' zscore: {z_score:.2f} ' + colorama.Style.RESET_ALL)
            else:
                print(colorama.Fore.BLACK + colorama.Back.WHITE + f' zscore: {z_score:.2f} ' + colorama.Style.RESET_ALL)
            print(f'1 price {self.stock1.close[0]}  2 price {self.stock2.close[0]}')

            if z_score > self.params.entry_z:
                if context.xiangguanxing == 1:
                    print(f'you can sell 1 buy 2')
                elif context.xiangguanxing == -1:
                    print(f'you can sell 1 sell 2')

            elif z_score < -self.params.entry_z:
                if context.xiangguanxing == 1:
                    print(f'you can buy 1 sell 2')
                elif context.xiangguanxing == -1:
                    print(f'you can buy 1 buy 2')
            elif abs(z_score) < self.params.exit_z:
                print(f'close')

            print('-----------------------------')

    def notify_order(self, order):
        if order.status is order.Completed:
            # print(order)
            pass

    def cal_hedge_ratio(self, close1, close2):
        try:
            prices_a = np.array(close1.get(size=self.params.window))
            prices_b = np.array(close2.get(size=self.params.window))
            X = sm.add_constant(prices_b)
            model = sm.OLS(prices_a, X)
            results = model.fit()
            return results.params[1]
        except Exception as e:
            pass


class DateFiller(bt.with_metaclass(bt.MetaParams, object)):
    def __init__(self, data):
        self.data = data
        self.last_valid_value = 2000
        # 记录最后一个有效值

    def __call__(self, data):
        if data.close[0] > 1:  # 当前数据有效
            self.last_valid_value = data.close[0]
        else:  # 当前数据缺失
            self.data.close[0] = self.last_valid_value  # 填充最后一个有效值


def main_process(futures_list):
    for item in futures_list:
        print('\n-----------------------------------------------------------')
        # df1 = data_processing.sina_daily_data('RB0')
        # df2 = data_processing.sina_daily_data('HC0')
        context.xiangguanxing = int(item[2])

        xishu1 = 1
        xishu2 = 1
        if len(item) > 4:
            xishu1 = int(item[3])
            xishu2 = int(item[4])

        goumai_xishu1 = 1
        goumai_xishu2 = 1

        if len(item) > 6:
            goumai_xishu1 = int(item[5])
            goumai_xishu2 = int(item[6])

        print(item)
        df1 = None
        df2 = None

        trytimes = 3
        while trytimes > 0:
            try:
                trytimes = trytimes - 1
                if context.use_daily is True:
                    df1 = data_processing.sina_daily_data(item[0])
                    df2 = data_processing.sina_daily_data(item[1])
                else:
                    df1 = data_processing.sina_minute_data(item[0], 60)
                    df2 = data_processing.sina_minute_data(item[1], 60)
            except Exception as e:
                time.sleep(2)
                continue

        cerebro = bt.Cerebro()

        # common_index = df1.index.intersection(df2.index)
        # df1 = df1.loc[common_index]
        # df2 = df2.loc[common_index]

        df1, df2 = data_processing.data_resample(df1, df2, xishu1 * goumai_xishu1, xishu2 * goumai_xishu2)

        length1 = len(df1)
        length2 = len(df2)
        if length1 < length2:
            context.zongchangdu = length1
        else:
            context.zongchangdu = length2

        context.window = int(context.zongchangdu * 75 / 100)

        data1 = bt.feeds.PandasData(dataname=df1)
        data2 = bt.feeds.PandasData(dataname=df2)

        data1.addfilter(DateFiller)
        data2.addfilter(DateFiller)

        cerebro.adddata(data1)
        cerebro.adddata(data2)

        # 添加策略
        cerebro.addstrategy(PairTradingStrategy)

        cerebro.broker.set_cash(100000)
        cerebro.broker.setcommission(commission=0.001)

        # 运行回测
        print('Starting Portfolio Value: %.2f, window %d' % (cerebro.broker.getvalue(), context.window))

        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio')
        results = cerebro.run()
        strat = results[0]

        if goumai_xishu1 > 1 or goumai_xishu2 > 1:
            print(
                colorama.Fore.BLACK + colorama.Back.WHITE + f'stock1 * {goumai_xishu1}, stock2 * {goumai_xishu2}' + colorama.Style.RESET_ALL)
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
        print(item)

        print(strat.analyzers.sharpe_ratio.get_analysis())
        print('-----------------------------------------------------------')
        # 绘制结果
        # cerebro.plot()

        time.sleep(1)

    with open('prompt.txt', 'r', encoding='utf-8') as file:
        prompt_content = file.read()

    print(prompt_content)
    print(f'******{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}***********\n')


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    futures_list = ast.literal_eval(config['tongjitaoli']['futures_list'])

    runtimes = 1
    data_dir = 'data_sina'
    sleep_time = 300
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--times', default=None)
    parser.add_argument('-d', '--dir', default=None)
    parser.add_argument('-s', '--sleep', default=None)
    parser.add_argument('-l', '--daily', choices=['true', 'false'], default='false')
    args = parser.parse_args()

    if args.times is not None:
        runtimes = int(args.times)

    if args.dir is not None:
        data_dir = args.dir

    if args.sleep is not None:
        sleep_time = int(args.sleep)

    if args.daily is not None:
        context.use_daily = args.daily == 'true'

    for i in range(0, runtimes):
        if os.path.exists(data_dir):
            current_time = datetime.datetime.now().time()

            if (datetime.time(9, 0) <= current_time <= datetime.time(10, 16)
                    or datetime.time(10, 30) <= current_time <= datetime.time(11, 32)
                    or datetime.time(13, 30) <= current_time <= datetime.time(15, 2)
                    or datetime.time(21, 0) <= current_time <= datetime.time(23, 59)
                    or datetime.time(0, 0) <= current_time <= datetime.time(1, 2)):
                if datetime.datetime.today().weekday() < 5:
                    shutil.rmtree(data_dir)

        try:
            main_process(futures_list)
        except Exception as e:
            print(e)
            print('main_process run error!!!!!!!')

        if runtimes > 1 and i < runtimes - 1:
            time.sleep(sleep_time)
    print('-------end---------\n')
