import akshare as ak


if __name__ == '__main__':
    try:
        with open('config.ini', 'r', encoding='utf-8') as file:
            for line in file:
                symbol_tmp = line.rstrip('\n')
                df = ak.stock_financial_hk_analysis_indicator_em(symbol=symbol_tmp, indicator="年度")
                #stock_financial_analysis_indicator_df['日期'] = stock_financial_analysis_indicator_df['日期'].astype(str)
                #df = stock_financial_analysis_indicator_df[stock_financial_analysis_indicator_df['日期'].str.contains('12-31', na=False)]
                df.to_csv(f'{symbol_tmp}.csv', index=False, encoding='utf-8-sig')
                print(df)
    except FileNotFoundError:
        print('not flond file')
    except Exception as e:
        print(f'read file error{e}')
    print('-------end---------\n')
