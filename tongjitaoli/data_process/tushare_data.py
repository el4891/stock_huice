import tushare as ts


pro = ts.pro_api('49991302f026b9c3e4120dc92d9221a171484deaa13e90b0417f80dc')

df = pro.df = pro.ft_mins(ts_code='CU2410.SHF', freq='60min')
print(df)