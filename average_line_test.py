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


def process_result_test(df, sell, run_index, result):
    zongjine = 0
    zonggushu = 0
    for i in range(df['日期'].size):
        if i > 0:
            if (df.iloc[i - 1, 2] < df.iloc[i - 1, 3]) \
                    and (df.iloc[i - 1, 3] < df.iloc[i - 1, 4]) \
                    and (df.iloc[i - 1, 4] < df.iloc[i - 1, 5]) \
                    and (df.iloc[i, 2] > df.iloc[i, 3]):
                mairu = 10000 / df.iloc[i, 1]
                mairu = mairu - mairu % 100
                zongjine = zongjine + df.iloc[i, 1] * mairu
                zonggushu = zonggushu + mairu
                # print('买入后')
                # print(zongjine)
                # print(zonggushu)
            elif (df.iloc[i - 1, 2] > df.iloc[i - 1, 3]) \
                    and (df.iloc[i - 1, 3] > df.iloc[i - 1, 4]) \
                    and (df.iloc[i - 1, 4] > df.iloc[i - 1, 5]) \
                    and (df.iloc[i, 2] < df.iloc[i, 3]) and (zonggushu > 0):
                maichu = zonggushu / sell
                maichu = maichu - maichu % 100
                if maichu < 100:
                    maichu = 100

                if maichu > zonggushu:
                    maichu = zonggushu

                zongjine = zongjine - df.iloc[i, 1] * maichu
                zonggushu = zonggushu - maichu
                # print('卖出后')
                # print(zongjine)
                # print(zonggushu)

    # print("sell " + str(sell) + " 平均股价：")
    # print(zongjine / zonggushu)
    # print(df.iloc[df['日期'].size - 1, 1])
    result.loc[run_index, '总金额'] = zongjine
    result.loc[run_index, '总股数'] = zonggushu
    if zonggushu > 0:
        result.loc[run_index, '平均股价'] = zongjine / zonggushu
    else:
        result.loc[run_index, '平均股价'] = 0.001
    result.loc[run_index, '当前股价'] = df.iloc[df['日期'].size - 1, 1]
    result.loc[run_index, '当前卖出盈利'] = result.loc[run_index, '当前股价'] * result.loc[run_index, '总股数'] - \
                                            result.loc[run_index, '总金额']


def process_data(filename, line1, line2, line3, line4, sell, run_index, result):
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
    process_result_test(df, sell, run_index, result)
    result.loc[run_index, '均线1'] = line1
    result.loc[run_index, '均线2'] = line2
    result.loc[run_index, '均线3'] = line3
    result.loc[run_index, '均线4'] = line4
    result.loc[run_index, '卖出除数'] = sell
    result.loc[run_index, '股票'] = filename

    print(result)
    # if not os.path.exists('out'):
    #     os.makedirs('out')
    #
    # df.to_csv('out/' + filename + '_result.csv')
    return 1


def line_product(filename):
    line1 = [1, 5]
    line2 = [8, 10]
    line3 = [80, 100]
    line4 = [120, 180]
    sell_persent = [10]

    run_index = 0
    result = pd.DataFrame(
        columns=['股票', '总金额', '总股数', '平均股价', '当前股价', '当前卖出盈利', '卖出除数', '均线1', '均线2',
                 '均线3', '均线4'])

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
                            # pool_line.apply_async(func=process_data, args=(filename, i, j, k, l, h, run_index, result))
                            process_data(filename, i, j, k, l, h, run_index, result)
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
