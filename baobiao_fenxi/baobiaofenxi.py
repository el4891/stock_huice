import argparse
import os
from pathlib import Path

import akshare as ak
import numpy as np
import pandas as pd


def shujudayin(df):
    print('----------------------------------------------------------------------------------------')
    print(df.iloc[:, 1:9])
    if '资产负债率(%)' in df.columns:
        print(f'资产负债率 {df['资产负债率(%)'].iloc[-1]}')
    elif 'DEBT_ASSET_RATIO' in df.columns:
        print(f'资产负债率 {df['DEBT_ASSET_RATIO'].iloc[-1]}')

    if '每股经营性现金流(元)' in df.columns:
        print(f'每股经营性现金流(元) {df['每股经营性现金流(元)'].iloc[-1]}')
    elif 'PER_NETCASH_OPERATE' in df.columns:
        print(f'每股经营性现金流(元) {df['PER_NETCASH_OPERATE'].iloc[-1]}')

    if '扣除非经常性损益后的每股收益(元)' in df.columns:
        print(f'扣除非经常性损益后的每股收益(元) {df['扣除非经常性损益后的每股收益(元)'].iloc[-1]}')
    elif 'DILUTED_EPS' in df.columns:
        print(f'扣除非经常性损益后的每股收益(元) {df['DILUTED_EPS'].iloc[-1]}')

    if '三项费用比重' in df.columns:
        print(f'三项费用比重 {df['三项费用比重'].iloc[-1]}')
    # elif 'DILUTED_EPS' in df.columns:
    #     print(f'三项费用比重 {df['DILUTED_EPS'].iloc[-1]}')

    if '净资产收益率(%)' in df.columns:
        print(f'净资产收益率(%) {df['净资产收益率(%)'].iloc[-1]}')
    elif 'ROE_AVG' in df.columns:
        print(f'净资产收益率(%) {df['ROE_AVG'].iloc[-1]}')

    if '应收账款周转率(次)' in df.columns:
        print(f'应收账款周转率(次) {df['应收账款周转率(次)'].iloc[-1]}')
    # elif 'DILUTED_EPS' in df.columns:
    #     print(f'应收账款周转率(次) {df['DILUTED_EPS'].iloc[-1]}')

    if '存货周转率(次)' in df.columns:
        print(f'存货周转率(次) {df['存货周转率(次)'].iloc[-1]}')
    # elif 'DILUTED_EPS' in df.columns:
    #     print(f'存货周转率(次) {df['DILUTED_EPS'].iloc[-1]}')

    if '速动比率' in df.columns:
        print(f'速动比率 {df['速动比率'].iloc[-1]}')
    # elif 'DILUTED_EPS' in df.columns:
    #     print(f'速动比率 {df['DILUTED_EPS'].iloc[-1]}')

    if '流动比率' in df.columns:
        print(f'流动比率 {df['流动比率'].iloc[-1]}')
    elif 'CURRENT_RATIO' in df.columns:
        print(f'流动比率 {df['CURRENT_RATIO'].iloc[-1]}')

    if '现金比率(%)' in df.columns:
        print(f'现金比率(%) {df['现金比率(%)'].iloc[-1]}')
    # elif 'DILUTED_EPS' in df.columns:
    #     print(f'现金比率(%) {df['DILUTED_EPS'].iloc[-1]}')

    print(symbol_tmp)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='config file')
    parser.add_argument('-f', '--file', required=False, help='config file')
    args = parser.parse_args()

    path = 'config.ini'
    if args.file != None:
        path = Path(args.file).resolve()

    try:
        with open(path, 'r', encoding='utf-8') as file:
            for line in file:
                symbol_tmp = line.rstrip('\n')
                df = None
                if not os.path.exists(f'{symbol_tmp}.csv'):
                    stock_code_len = len(symbol_tmp)
                    if stock_code_len == 6:
                        df = ak.stock_financial_analysis_indicator(symbol=symbol_tmp, start_year="2022")
                        df['日期'] = df['日期'].astype(str)
                        df = df[df['日期'].str.contains('12-31', na=False)]
                    elif stock_code_len < 6:
                        df = ak.stock_financial_hk_analysis_indicator_em(symbol=symbol_tmp, indicator="年度")
                        df = df.head(5)

                    numeric_cols = df.select_dtypes(include=[np.number]).columns
                    averages = df[numeric_cols].mean()
                    df = pd.concat([df, averages.to_frame().T.rename(index={0: 'average'})], ignore_index=True)
                    df.to_csv(f'{symbol_tmp}.csv', index=False, encoding='utf-8-sig')
                else:
                    df = pd.read_csv(f'{symbol_tmp}.csv')
                shujudayin(df)
    except FileNotFoundError:
        print('not flond file')
    except Exception as e:
        print(f'read file error{e}')
    print('-------end---------\n')
