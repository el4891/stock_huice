import os

import akshare as ak
import pandas as pd
import numpy as np

from pathlib import Path
import argparse

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

                    numeric_cols = df.select_dtypes(include=[np.number]).columns
                    averages = df[numeric_cols].mean()
                    df = pd.concat([df, averages.to_frame().T.rename(index={0:'average'})], ignore_index=True)
                    df.to_csv(f'{symbol_tmp}.csv', index=False, encoding='utf-8-sig')
                else:
                    df = pd.read_csv(f'{symbol_tmp}.csv')
                print(df)
    except FileNotFoundError:
        print('not flond file')
    except Exception as e:
        print(f'read file error{e}')
    print('-------end---------\n')
