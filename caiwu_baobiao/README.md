# caiwu_baobiao

下载完整财务报表（资产负债表、利润表、现金流量表），存为 CSV。

## 依赖

```
pip install akshare pandas
```

## 用法

```bash
python xiazai_baobiao.py                        # 读取 config.ini，输出到 ./output
python xiazai_baobiao.py -f codes.ini -o out    # 指定清单与输出目录
python xiazai_baobiao.py --freq yearly --force  # 只下年度数据并覆盖已有文件
```

参数说明：

| 参数 | 说明 |
| --- | --- |
| `-f, --file` | 股票代码清单，默认本目录 `config.ini` |
| `-o, --outdir` | 输出目录，默认本目录 `output` |
| `--freq` | `report`=按报告期，`yearly`=按年度，`both`=两者（默认） |
| `--force` | 已存在的文件也重新下载 |
| `--sleep` | 每次请求间隔秒数，默认 1.0 |

## 数据来源与接口

东方财富（akshare）：

- A 股：`stock_balance_sheet_by_report_em`、`stock_profit_sheet_by_report_em`、`stock_cash_flow_sheet_by_report_em` 及对应的 `_by_yearly_em`
- 港股：`stock_financial_hk_report_em`（`symbol` 取 `资产负债表`/`利润表`/`现金流量表`，`indicator` 取 `年度`/`报告期`）

## 代码识别规则

- 6 位数字：A 股。`60`/`68`/`9` 开头 → `SH`，`00`/`30`/`20` 开头 → `SZ`，`4`/`8` 开头 → `BJ`
- 少于 6 位（如 `00700`）：港股

## 输出

```
output/{代码}_{报表名}_{report|yearly}.csv
```

例：`output/600519_资产负债表_report.csv`，编码 `utf-8-sig`，可直接用 Excel 打开。

表头为中文，映射表在 `field_names.py`：
- 东方财富返回的英文字段（`SECUCODE`、`SECURITY_CODE` 等）在保存前统一替换为中文；
- 以 `_YOY` 结尾的同比字段自动命名为「<中文名>同比增长率」；
- 极少数生僻字段（如 `FIN_FUND`、`ACF_END_INCOME`）暂未确认中文含义，保留英文原名，可在 `field_names.py` 中补充。

单个代码或报表失败不会中断整批，末尾会汇总失败清单。
