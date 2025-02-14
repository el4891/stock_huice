import os
import traceback
from concurrent.futures import ProcessPoolExecutor

import pandas as pd

from data_process.data_processing import DataProcessing


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


def process():
    filelist = None
    for i, j, k in os.walk('line'):
        filelist = k
        print(filelist)

    with ProcessPoolExecutor(max_workers=16) as executor:
        executor.map(line_product, filelist)


if __name__ == '__main__':
    process()
