import akshare as ak


if __name__ == '__main__':
    try:
        with open('config.ini', 'r', encoding='utf-8') as file:
            for line in file:
                symbol_tmp = line.rstrip('\n')
                stock_financial_analysis_indicator_df = ak.stock_financial_analysis_indicator(symbol=symbol_tmp, start_year="2022")
                stock_financial_analysis_indicator_df.to_csv(f'{symbol_tmp}.csv', index=False, encoding='utf-8-sig')
    except FileNotFoundError:
        print('not flond file')
    except Exception as e:
        print(f'read file error{e}')
    print('-------end---------\n')
