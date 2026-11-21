import baostock as bs
import pandas as pd

# 登录 Baostock 系统
lg = bs.login()

if lg.error_code == '0':
    print("登录成功")

    # 定义期货合约代码（示例：螺纹钢主力合约 rb2205）
    symbol = "sh.603288"  # 需替换为实际合约代码

    #### 获取沪深A股历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
    # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
    # 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
    rs = bs.query_history_k_data_plus(symbol,
                                      "date,time,code,open,high,low,close,volume,amount,adjustflag",
                                      start_date='2024-07-01', end_date='2024-12-31',
                                      frequency="5", adjustflag="3")
    print('query_history_k_data_plus respond error_code:' + rs.error_code)
    print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)

    #### 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)

    #### 结果集输出到csv文件 ####
    # result.to_csv("D:\\history_A_stock_k_data.csv", index=False)
    print(result)

    #### 登出系统 ####
    bs.logout()
else:
    print(f"登录失败：{lg.error_msg}")
