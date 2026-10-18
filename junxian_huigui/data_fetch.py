"""行情数据获取：A 股 / 港股日线行情，数据源 akshare，带本地 CSV 缓存。

对外主接口：
    fetch_daily(symbol, start_date, end_date, market='auto', adjust='qfq', use_cache=True)
返回 (DataFrame, 规范化代码, 实际市场)，DataFrame 列为 datetime/open/high/low/close/volume。
"""

import os
import re

import akshare as ak
import pandas as pd

# 缓存目录与脚本同级的 data_cache
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data_cache')

# akshare 返回的中文列名 -> 回测统一使用的列名
COLUMN_MAP = {
    '日期': 'datetime',
    '开盘': 'open',
    '最高': 'high',
    '最低': 'low',
    '收盘': 'close',
    '成交量': 'volume',
}

REQUIRED_COLUMNS = ['datetime', 'open', 'high', 'low', 'close', 'volume']

VALID_ADJUST = {'qfq': '前复权', 'hfq': '后复权', '': '不复权'}


def detect_market(symbol):
    """根据代码长度推断市场：6 位及以上为 A 股，5 位及以下为港股。"""
    code = re.sub(r'\D', '', str(symbol or ''))
    if not code:
        raise ValueError('证券代码不能为空')
    return 'A' if len(code) >= 6 else 'HK'


def normalize_symbol(symbol, market='auto'):
    """规范化证券代码，返回 (代码, 市场)。

    A 股为 6 位数字，港股为 5 位数字（不足补零）。
    """
    code = re.sub(r'\D', '', str(symbol or ''))
    if not code:
        raise ValueError('证券代码不能为空')

    market = str(market or 'auto').upper()
    if market in ('AUTO', '自动'):
        market = detect_market(code)

    if market == 'A':
        if len(code) > 6:
            raise ValueError('A 股代码应为 6 位数字，例如 600519')
        return code.zfill(6), 'A'

    if market == 'HK':
        if len(code) > 5:
            raise ValueError('港股代码应为 5 位数字，例如 00700')
        return code.zfill(5), 'HK'

    raise ValueError('未知市场：%s' % market)


def market_name(market):
    """市场代码转中文名称。"""
    return 'A股' if market == 'A' else '港股'


def _format_date(value):
    """把日期统一为 akshare 需要的 YYYYMMDD。"""
    return pd.Timestamp(value).strftime('%Y%m%d')


def _cache_path(code, market, start, end, adjust):
    """缓存文件名包含代码、市场、区间与复权方式，避免不同参数互相串用。"""
    adjust_key = adjust or 'none'
    return os.path.join(CACHE_DIR, '%s_%s_%s_%s_%s.csv' % (market, code, start, end, adjust_key))


def _a_symbol_with_prefix(code):
    """新浪 A 股接口需要交易所前缀：6 开头沪市，0/3 开头深市，4/8 开头北交所。"""
    if code.startswith('6'):
        return 'sh' + code
    if code.startswith(('0', '3')):
        return 'sz' + code
    if code.startswith(('4', '8')):
        return 'bj' + code
    return 'sh' + code


def _fetch_a_raw(code, start, end, adjust):
    """A 股行情：优先东方财富接口，失败时回退新浪接口。

    东方财富接口偶发断连（RemoteDisconnected），新浪接口列名为 date 且需要交易所前缀。
    """
    try:
        return ak.stock_zh_a_hist(symbol=code, period='daily', start_date=start,
                                  end_date=end, adjust=adjust)
    except Exception as primary_exc:
        try:
            raw = ak.stock_zh_a_daily(symbol=_a_symbol_with_prefix(code), start_date=start,
                                      end_date=end, adjust=adjust)
        except Exception as backup_exc:
            raise RuntimeError('东方财富接口与新浪接口均获取失败：%s / %s'
                               % (primary_exc, backup_exc)) from backup_exc
        if raw is None or raw.empty:
            raise primary_exc
        raw = raw.rename(columns={'date': 'datetime'})
        raw['datetime'] = pd.to_datetime(raw['datetime'])
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        return raw[(raw['datetime'] >= start_ts) & (raw['datetime'] <= end_ts)].reset_index(drop=True)


def _fetch_hk_raw(code, start, end, adjust):
    """港股行情：优先东方财富接口，失败时回退新浪接口。

    新浪接口返回全历史数据且列名为 date，需按区间截取。
    """
    try:
        return ak.stock_hk_hist(symbol=code, period='daily', start_date=start,
                                end_date=end, adjust=adjust)
    except Exception as primary_exc:
        try:
            raw = ak.stock_hk_daily(symbol=code, adjust=adjust)
        except Exception as backup_exc:
            raise RuntimeError('东方财富接口与新浪接口均获取失败：%s / %s'
                               % (primary_exc, backup_exc)) from backup_exc
        if raw is None or raw.empty:
            raise primary_exc
        raw = raw.rename(columns={'date': 'datetime'})
        raw['datetime'] = pd.to_datetime(raw['datetime'])
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        return raw[(raw['datetime'] >= start_ts) & (raw['datetime'] <= end_ts)].reset_index(drop=True)


def fetch_daily(symbol, start_date, end_date, market='auto', adjust='qfq', use_cache=True):
    """获取日线行情。

    参数：
        symbol      证券代码，A 股 6 位、港股 5 位
        start_date  开始日期，支持 'YYYY-MM-DD' 字符串
        end_date    结束日期
        market      'auto' / 'A' / 'HK'
        adjust      'qfq' / 'hfq' / ''（不复权）
        use_cache   是否优先读取本地缓存

    返回：(DataFrame, 规范化代码, 市场)
    """
    code, real_market = normalize_symbol(symbol, market)
    start = _format_date(start_date)
    end = _format_date(end_date)
    if start > end:
        raise ValueError('开始日期不能晚于结束日期')

    adjust = str(adjust or '').lower()
    if adjust not in VALID_ADJUST:
        raise ValueError('复权方式无效：%s' % adjust)

    cache_file = _cache_path(code, real_market, start, end, adjust)

    if use_cache and os.path.exists(cache_file):
        df = pd.read_csv(cache_file, parse_dates=['datetime'])
    else:
        try:
            if real_market == 'A':
                raw = _fetch_a_raw(code, start, end, adjust)
            else:
                raw = _fetch_hk_raw(code, start, end, adjust)
        except Exception as exc:
            raise RuntimeError('获取 %s 行情失败：%s' % (code, exc)) from exc

        if raw is None or raw.empty:
            raise ValueError('未获取到行情数据：%s（%s ~ %s）' % (code, start, end))

        df = raw.rename(columns=COLUMN_MAP)
        missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError('行情数据缺少字段：%s' % '、'.join(missing))

        df = df[REQUIRED_COLUMNS].copy()
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime').drop_duplicates('datetime').reset_index(drop=True)

        if use_cache:
            os.makedirs(CACHE_DIR, exist_ok=True)
            df.to_csv(cache_file, index=False)

    if df is None or df.empty:
        raise ValueError('行情数据为空：%s（%s ~ %s）' % (code, start, end))

    return df, code, real_market
