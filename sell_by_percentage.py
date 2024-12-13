import inspect
import multiprocessing
import os
from datetime import datetime

import pandas as pd

import average_line


def print_line_number():
    frame = inspect.currentframe()
    lineno = frame.f_back.f_lineno
    print(f'Line{lineno}')


def process_result_test(df, sell, peak_persent, run_index, result):
    zongjine = 0
    zuida_jine = zongjine
    zonggushu = 0
    peak_price = 0.0
    for i in range(df['日期'].size):
        if i > 0:
            current_price = df.iloc[i, 1]
            if (df.iloc[i - 1, 2] < df.iloc[i - 1, 3]) \
                    and (df.iloc[i - 1, 3] < df.iloc[i - 1, 4]) \
                    and (df.iloc[i - 1, 4] < df.iloc[i - 1, 5]) \
                    and (df.iloc[i, 2] > df.iloc[i, 3]):
                mairu = 100000 / current_price
                mairu = mairu - (mairu % 100)
                zongjine = zongjine + current_price * mairu
                zonggushu = zonggushu + mairu
                if zongjine > zuida_jine:
                    zuida_jine = zongjine
                # print('买入后 ' + df.iloc[i, 0])
                # print('当前股价：' + str(current_price) + ' 买入股数 ' + str(mairu) + ' 总金额 ' + str(
                #     zongjine) + ' 总股数 ' + str(zonggushu))
            # 这里是卖出策略
            elif (df.iloc[i, 2] > df.iloc[i, 4]) and (df.iloc[i, 4] > df.iloc[i, 5]) and (zonggushu > 0):
                if (peak_price > 0.001) and (current_price < peak_price * (1 - peak_persent)):
                    maichu = zonggushu / sell
                    maichu = maichu - maichu % 100
                    if maichu < 100:
                        maichu = 100

                    if maichu > zonggushu:
                        maichu = zonggushu

                    zongjine = zongjine - current_price * maichu
                    zonggushu = zonggushu - maichu

                    # print('卖出后' + df.iloc[i, 0])
                    # print('当前股价：' + str(current_price) + ' 卖出股数 ' + str(maichu) + ' 总金额 ' + str(zongjine)
                    #       + ' 总股数 ' + str(zonggushu) + ' 价格峰值 ' + str(peak_price))

                    peak_price = 0
                elif current_price > peak_price:
                    peak_price = current_price
            else:
                peak_price = 0

    # print("sell " + str(sell) + " 平均股价：")
    # print(zongjine / zonggushu)
    # print(df.iloc[df['日期'].size - 1, 1])
    result.loc[run_index, '总金额'] = zongjine
    result.loc[run_index, '最大投入额'] = zuida_jine
    result.loc[run_index, '总股数'] = zonggushu
    result.loc[run_index, '交易天数'] = df['日期'].size
    if zonggushu > 0:
        result.loc[run_index, '平均股价'] = zongjine / zonggushu
    else:
        result.loc[run_index, '平均股价'] = 0.001
    result.loc[run_index, '当前股价'] = df.iloc[df['日期'].size - 1, 1]
    result.loc[run_index, '当前卖出盈利'] = result.loc[run_index, '当前股价'] * result.loc[run_index, '总股数'] - \
                                            result.loc[run_index, '总金额']
    result.loc[run_index, '日均盈利'] = result.loc[run_index, '当前卖出盈利'] / result.loc[run_index, '交易天数']
    result.loc[run_index, '参考日盈'] = result.loc[run_index, '最大投入额'] * 0.2 / 236


def process_data(filename, line1, line2, line3, line4, sell, peak_persent, run_index, result):
    filepath = 'day_line/' + filename
    df = pd.read_csv(filepath, header=1)
    df = df.iloc[:-1]

    line1_average = average_line.get_average_line(df, line1)
    line2_average = average_line.get_average_line(df, line2)
    line3_average = average_line.get_average_line(df, line3)
    line4_average = average_line.get_average_line(df, line4)

    df = df[['日期', '收盘']]
    df = pd.merge(df, line1_average, on='日期')
    df = pd.merge(df, line2_average, on='日期')
    df = pd.merge(df, line3_average, on='日期')
    df = pd.merge(df, line4_average, on='日期')

    # print(str(line1) + ' ' + str(line2) + ' ' + str(line3) + ' ' + str(line4) + ' ' + str(sell))
    process_result_test(df, sell, peak_persent, run_index, result)
    result.loc[run_index, '均线1'] = line1
    result.loc[run_index, '均线2'] = line2
    result.loc[run_index, '均线3'] = line3
    result.loc[run_index, '均线4'] = line4
    result.loc[run_index, '卖出除数'] = sell
    result.loc[run_index, '回落卖出'] = peak_persent
    result.loc[run_index, '股票'] = filename

    print(result)
    # if not os.path.exists('out'):
    #     os.makedirs('out')
    #
    # df.to_csv('out/' + filename + '_result.csv')
    return 1


def line_product(filename):
    # line1 = [1]
    # line2 = [8]
    # line3 = [80]
    # line4 = [180]
    # sell_persent = [10]
    # peak_persent = [0.08]

    line1 = [1]
    line2 = [8]
    line3 = [80]
    line4 = [180]
    sell_persent = [10]
    peak_persent = [0.08]

    run_index = 0
    result = pd.DataFrame(
        columns=['股票', '总金额', '总股数', '平均股价', '当前股价', '当前卖出盈利', '交易天数', '日均盈利', '参考日盈',
                 '最大投入额', '卖出除数', '回落卖出', '均线1',
                 '均线2', '均线3', '均线4'])

    # pool_line = multiprocessing.Pool(multiprocessing.cpu_count())

    # products = [process_data(filename, i, j, k, l, h) for i, j, k, l, h in
    #             product(line1, line2, line3, line4, sell_persent)]
    # products = [pool_line.apply_async(func=process_data, args=(filename, i, j, k, l, h, run_index, result)) for i, j, k, l, h in
    #             product(line1, line2, line3, line4, sell_persent)]
    for i in line1:
        for j in line2:
            if i < j:
                for k in line3:
                    for l in line4:
                        for h in sell_persent:
                            for n in peak_persent:
                                # pool_line.apply_async(func=process_data, args=(filename, i, j, k, l, h, run_index, result))
                                process_data(filename, i, j, k, l, h, n, run_index, result)
                                run_index = run_index + 1

    # 获取当前时间并格式化为字符串
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # 生成的目录路径
    directory = "out"
    if not os.path.exists(directory):
        os.makedirs(directory)

    result = result.sort_values('当前卖出盈利', ascending=False)
    result.to_csv(f'{directory}/{filename}_{now}_result.csv')
    # pool_line.close()
    # pool_line.join()


def process():
    filelist = None
    for i, j, k in os.walk('day_line'):
        filelist = k
        print(filelist)

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map_async(process_data, filelist)
    for filename in filelist:
        pool.apply_async(func=line_product, args=(filename,))
        # line_product(filename)
    pool.close()
    pool.join()

if __name__ == '__main__':
    process()
