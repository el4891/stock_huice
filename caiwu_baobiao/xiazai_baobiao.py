"""下载完整财务报表（资产负债表 / 利润表 / 现金流量表）。

数据来源：东方财富（通过 akshare）
- A 股：stock_<表>_sheet_by_report_em / stock_<表>_sheet_by_yearly_em
- 港股：stock_financial_hk_report_em（symbol 取 资产负债表/利润表/现金流量表）

用法：
    python xiazai_baobiao.py                        # 读取本目录 config.ini，输出到 ./output
    python xiazai_baobiao.py -f codes.ini -o out    # 指定股票清单与输出目录
    python xiazai_baobiao.py --freq yearly --force  # 只下载年度数据并覆盖已存在文件
"""
import argparse
import sys
import time
from pathlib import Path

import akshare as ak
import pandas as pd

SHEETS = ('资产负债表', '利润表', '现金流量表')

# 报表名 -> (按报告期接口名, 按年度接口名)
SHEET_FUNCS = {
    '资产负债表': ('stock_balance_sheet_by_report_em', 'stock_balance_sheet_by_yearly_em'),
    '利润表': ('stock_profit_sheet_by_report_em', 'stock_profit_sheet_by_yearly_em'),
    '现金流量表': ('stock_cash_flow_sheet_by_report_em', 'stock_cash_flow_sheet_by_yearly_em'),
}


def em_symbol(code):
    """6 位 A 股代码转带市场标识的代码；非 6 位返回 None（按港股处理）。"""
    if len(code) != 6 or not code.isdigit():
        return None
    if code.startswith(('60', '68', '9')):
        return f'SH{code}'
    if code.startswith(('00', '30', '20')):
        return f'SZ{code}'
    if code.startswith(('4', '8')):
        return f'BJ{code}'
    raise ValueError(f'无法识别的 A 股代码: {code}')


def read_codes(path):
    codes = []
    with open(path, 'r', encoding='utf-8') as file:
        for line in file:
            code = line.strip()
            if code and not code.startswith('#'):
                codes.append(code)
    return codes


def fetch(code, sheet, freq):
    """拉取单张报表，A 股按市场标识，港股走港股接口。"""
    symbol = em_symbol(code)
    if symbol is None:
        indicator = '年度' if freq == 'yearly' else '报告期'
        return ak.stock_financial_hk_report_em(stock=code, symbol=sheet, indicator=indicator)

    func_name = SHEET_FUNCS[sheet][0 if freq == 'report' else 1]
    return getattr(ak, func_name)(symbol=symbol)


def main():
    parser = argparse.ArgumentParser(description='下载完整财务报表')
    parser.add_argument('-f', '--file', help='股票代码清单文件，默认本目录 config.ini')
    parser.add_argument('-o', '--outdir', help='输出目录，默认本目录 output')
    parser.add_argument('--freq', choices=('report', 'yearly', 'both'), default='both',
                        help='report=按报告期, yearly=按年度, both=两者都下（默认）')
    parser.add_argument('--force', action='store_true', help='已存在的文件也重新下载')
    parser.add_argument('--sleep', type=float, default=1.0, help='每次请求间隔秒数，默认 1.0')
    args = parser.parse_args()

    base = Path(__file__).resolve().parent
    config = Path(args.file).resolve() if args.file else base / 'config.ini'
    outdir = Path(args.outdir).resolve() if args.outdir else base / 'output'
    outdir.mkdir(parents=True, exist_ok=True)

    try:
        codes = read_codes(config)
    except FileNotFoundError:
        print(f'not flond file: {config}')
        return
    except Exception as e:
        print(f'read file error {e}')
        return

    freqs = ('report', 'yearly') if args.freq == 'both' else (args.freq,)
    ok, failed = 0, []

    for code in codes:
        for sheet in SHEETS:
            for freq in freqs:
                target = outdir / f'{code}_{sheet}_{freq}.csv'
                if target.exists() and not args.force:
                    print(f'skip(已存在): {target.name}')
                    continue
                try:
                    df = fetch(code, sheet, freq)
                except Exception as e:
                    print(f'fail: {code} {sheet} {freq} -> {e}')
                    failed.append(f'{code} {sheet} {freq}')
                    continue
                if not isinstance(df, pd.DataFrame) or df.empty:
                    print(f'empty: {code} {sheet} {freq}')
                    failed.append(f'{code} {sheet} {freq}')
                    continue
                df.to_csv(target, index=False, encoding='utf-8-sig')
                ok += 1
                print(f'saved: {target.name} ({len(df)} 行)')
                time.sleep(args.sleep)

    print('----------------------------------------------------------------------------------------')
    print(f'完成 {ok} 个文件，失败 {len(failed)} 个')
    for item in failed:
        print(f'  失败: {item}')
    print('-------end---------\n')


if __name__ == '__main__':
    sys.exit(main())
