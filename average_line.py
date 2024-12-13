import pandas as pd


def get_average_line(df: pd.DataFrame, days):
    col_name = str(days) + '_' + '平均'
    data_return = pd.DataFrame(columns=['日期', col_name])
    for i in range(df['收盘'].size - days + 1):
        data_return.loc[i, '日期'] = df.iloc[i + days - 1, 0]
        data_return.loc[i, col_name] = df.iloc[i:i + days, 4].sum() / df.iloc[i:i + days, 4].size
    return data_return
