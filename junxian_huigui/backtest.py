"""均线回归回测引擎：组织 backtrader 运行并汇总盈亏、成交次数与买卖时点。

命令行示例：
    python backtest.py 600519 --start 2020-01-01 --end 2024-12-31 \\
        --ma 5 20 60 --buy "0>1,1<2" --sell "0<1,1>2" --cash 10000
"""

import argparse
import re
import sys

import backtrader as bt
import pandas as pd

from data_fetch import fetch_daily, market_name
from ma_strategy import ThreeMaStrategy

CONDITION_RE = re.compile(r'(\d)\s*([><])\s*(\d)')

VALID_OPS = ('>', '<')

# 分批买入的触发方式：fall=较上次买价下跌，rise=较上次买价上涨
BUY_MODES = ('fall', 'rise')

# 分批卖出的触发方式：rise=较上次卖价上涨，high=较持仓期间最高价回落
SELL_MODES = ('rise', 'high')

# 成交时机：immediate=条件成立即成交（旧方式），step=等涨跌幅达标再成交
SIGNAL_MODES = ('step', 'immediate')

# 初始资金下限：倍数设得过小时兜底，避免连续加仓时资金不足被拒单
MIN_INITIAL_CASH = 100000.0


def parse_conditions(text):
    """解析 '0>1,1<2' 形式的条件串，返回 ((0, '>', 1), (1, '<', 2))。"""
    conditions = []
    for part in str(text or '').replace('，', ',').split(','):
        part = part.strip()
        if not part:
            continue
        match = CONDITION_RE.fullmatch(part)
        if not match:
            raise ValueError('条件格式错误：%s（示例：0>1,1<2）' % part)
        conditions.append((int(match.group(1)), match.group(2), int(match.group(3))))
    return tuple(conditions)


def _validate_conditions(conditions, name, ma_count=3):
    """校验条件：索引在均线范围内、比较符合法，且至少有一条。"""
    conditions = tuple(conditions or ())
    if not conditions:
        raise ValueError('%s至少需要设置一条均线条件' % name)
    for item in conditions:
        if len(item) != 3:
            raise ValueError('%s格式错误：%s' % (name, item))
        left, op, right = int(item[0]), item[1], int(item[2])
        if not (0 <= left < ma_count and 0 <= right < ma_count):
            raise ValueError('%s引用了不存在的均线：MA%d' % (name, max(left, right) + 1))
        if op not in VALID_OPS:
            raise ValueError('%s比较符只能是 > 或 <：%s' % (name, op))
    return conditions


def run_backtest(df, ma_periods, buy_conditions, sell_conditions, ma_type='SMA',
                 cash_per_trade=10000.0, initial_cash_multiple=20.0,
                 lot_size=100, commission=0.0005,
                 symbol='', market='A', step_buy_pct=0.0, step_sell_pct=0.0,
                 sell_fraction=1.0, sell_mode='rise', buy_mode='fall',
                 signal_mode='step'):
    """执行回测并返回结果字典。

    df 需包含 datetime/open/high/low/close/volume 六列。
    initial_cash_multiple 为初始资金相对「每次买入金额」的倍数（须大于 0）；
    signal_mode 为成交时机：immediate=条件成立即成交，step=条件成立只更新基准、
    价格相对基准变动达到间隔后才成交；
    step_buy_pct / step_sell_pct 为买卖的价格间隔（百分比），0 表示条件成立即成交；
    buy_mode 为买入触发方式：fall=较基准下跌，rise=较基准上涨；
    sell_mode 为卖出触发方式：rise=较基准上涨，high=较持仓高点回落；
    sell_fraction 为每次卖出占持仓的比例（0 < 比例 <= 1）。
    """
    if df is None or df.empty:
        raise ValueError('行情数据为空，无法回测')

    ma_periods = tuple(int(p) for p in ma_periods)
    if len(ma_periods) != 3 or min(ma_periods) <= 0:
        raise ValueError('需要三条均线，且周期必须为正整数')

    buy_conditions = _validate_conditions(buy_conditions, '买入条件')
    sell_conditions = _validate_conditions(sell_conditions, '卖出条件')

    cash_per_trade = float(cash_per_trade)
    if cash_per_trade <= 0:
        raise ValueError('每次买入金额必须大于 0')
    initial_cash_multiple = float(initial_cash_multiple)
    if initial_cash_multiple <= 0:
        raise ValueError('初始资金倍数必须大于 0')
    lot_size = max(int(lot_size), 1)
    commission = max(float(commission), 0.0)

    # 分批买卖参数：间隔为负视为 0（不启用），卖出比例必须落在 (0, 1]
    step_buy_pct = max(float(step_buy_pct), 0.0)
    step_sell_pct = max(float(step_sell_pct), 0.0)
    sell_fraction = float(sell_fraction)
    if not 0.0 < sell_fraction <= 1.0:
        raise ValueError('每次卖出比例必须在 0 ~ 1 之间')
    buy_mode = str(buy_mode or 'fall').lower()
    if buy_mode not in BUY_MODES:
        raise ValueError('买入触发方式只能是 fall（每下跌）或 rise（每上涨）：%s' % buy_mode)
    sell_mode = str(sell_mode or 'rise').lower()
    if sell_mode not in SELL_MODES:
        raise ValueError('卖出触发方式只能是 rise（每上涨）或 high（从高点回落）：%s' % sell_mode)
    signal_mode = str(signal_mode or 'step').lower()
    if signal_mode not in SIGNAL_MODES:
        raise ValueError('成交时机只能是 immediate（条件成立即成交）或 step（等涨跌幅）：%s'
                         % signal_mode)

    frame = df.copy()
    frame['datetime'] = pd.to_datetime(frame['datetime'])
    for col in ('open', 'high', 'low', 'close', 'volume'):
        frame[col] = pd.to_numeric(frame[col], errors='coerce')
    frame = (frame.dropna(subset=['open', 'high', 'low', 'close'])
                  .sort_values('datetime')
                  .drop_duplicates('datetime')
                  .set_index('datetime'))
    if frame.empty:
        raise ValueError('有效行情数据为空，无法回测')

    feed = bt.feeds.PandasData(dataname=frame[['open', 'high', 'low', 'close', 'volume']])

    # 持仓期间买入条件仍成立时会持续加仓，初始资金按「每次买入金额 × 倍数」预留，
    # 倍数由资金设置界面指定；不足下限时按 MIN_INITIAL_CASH 兜底
    initial_cash = max(cash_per_trade * initial_cash_multiple, MIN_INITIAL_CASH)

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.adddata(feed, name=symbol or 'data')
    cerebro.addstrategy(
        ThreeMaStrategy,
        ma_periods=ma_periods,
        ma_type=ma_type,
        buy_conditions=buy_conditions,
        sell_conditions=sell_conditions,
        cash_per_trade=cash_per_trade,
        lot_size=lot_size,
        commission=commission,
        step_buy_pct=step_buy_pct,
        step_sell_pct=step_sell_pct,
        sell_fraction=sell_fraction,
        sell_mode=sell_mode,
        buy_mode=buy_mode,
        signal_mode=signal_mode,
    )
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=commission)

    strategies = cerebro.run()
    strat = strategies[0]

    trades = list(strat.trades)
    realized = sum(item['pnl'] for item in trades)
    open_position = strat.open_position
    unrealized = open_position['pnl'] if open_position else 0.0

    win_count = sum(1 for item in trades if item['pnl'] > 0)
    lose_count = sum(1 for item in trades if item['pnl'] <= 0)

    buy_count = sum(1 for item in strat.order_log if item['side'] == 'buy')
    sell_count = sum(1 for item in strat.order_log if item['side'] == 'sell')

    return dict(
        symbol=symbol,
        market=market,
        market_name=market_name(market) if market in ('A', 'HK') else str(market),
        start=frame.index[0].to_pydatetime(),
        end=frame.index[-1].to_pydatetime(),
        bars=len(frame),
        ma_periods=ma_periods,
        ma_type=ma_type,
        buy_conditions=buy_conditions,
        sell_conditions=sell_conditions,
        cash_per_trade=cash_per_trade,
        initial_cash_multiple=initial_cash_multiple,
        lot_size=lot_size,
        commission=commission,
        step_buy_pct=step_buy_pct,
        step_sell_pct=step_sell_pct,
        sell_fraction=sell_fraction,
        sell_mode=sell_mode,
        buy_mode=buy_mode,
        signal_mode=signal_mode,
        initial_cash=initial_cash,
        final_value=cerebro.broker.getvalue(),
        realized_pnl=realized,
        unrealized_pnl=unrealized,
        total_pnl=realized + unrealized,
        commission_total=sum(item['comm'] for item in strat.order_log),
        buy_count=buy_count,
        sell_count=sell_count,
        closed_count=len(trades),
        win_count=win_count,
        lose_count=lose_count,
        win_rate=(win_count / len(trades) * 100) if trades else 0.0,
        orders=list(strat.order_log),
        anchors=dict(strat.anchor_state),
        trades=trades,
        open_position=open_position,
        skipped_count=strat.skipped_count,
        failed_buys=list(strat.failed_buys),
        failed_buy_count=strat.failed_buy_count,
        messages=list(strat.messages),
    )


def describe_conditions(conditions, ma_periods):
    """把条件元组转成 MA5 > MA20 这样的可读文本。"""
    parts = []
    for left, op, right in conditions:
        parts.append('MA%d %s MA%d' % (ma_periods[int(left)], op, ma_periods[int(right)]))
    return ' 且 '.join(parts)


def fraction_text(fraction):
    """把卖出比例转成 1/4、1/3、1/2、全部 这样的文本。"""
    value = float(fraction)
    for candidate, text in ((1.0, '全部'), (0.5, '1/2'), (1.0 / 3, '1/3'), (0.25, '1/4')):
        if abs(value - candidate) < 1e-6:
            return text
    return '%.2f%%' % (value * 100)


def describe_batch(step_buy_pct, step_sell_pct, sell_fraction, sell_mode='rise',
                   buy_mode='fall', signal_mode='step'):
    """把成交时机与分批买卖设置转成可读文本，返回 (买入说明, 卖出说明)。"""
    immediate = str(signal_mode) == 'immediate'
    if step_buy_pct <= 0:
        buy_text = '条件成立即买入（不限制间隔）'
    elif immediate:
        trend = '上涨' if str(buy_mode) == 'rise' else '下跌'
        buy_text = ('条件成立即买入，之后每%s %.2f%% 加仓一次'
                    % (trend, step_buy_pct))
    else:
        trend = '上涨' if str(buy_mode) == 'rise' else '下跌'
        buy_text = ('条件成立后以当时价格为基准，再%s %.2f%% 买入一次'
                    % (trend, step_buy_pct))

    if step_sell_pct <= 0:
        sell_text = '信号成立时全部卖出'
    else:
        if str(sell_mode) == 'high':
            if immediate:
                trigger = '从高点回落 %.2f%% 时' % step_sell_pct
            else:
                trigger = '条件成立后从当时高点回落 %.2f%% 时' % step_sell_pct
        else:
            if immediate:
                trigger = '条件成立即先卖一次，之后每上涨 %.2f%% 时' % step_sell_pct
            else:
                trigger = '条件成立后以当时价格为基准再上涨 %.2f%% 时' % step_sell_pct
        if float(sell_fraction) >= 1.0:
            sell_text = '%s全部卖出' % trigger
        else:
            sell_text = '%s卖出持仓的 %s' % (trigger, fraction_text(sell_fraction))
    return buy_text, sell_text


def main(argv=None):
    parser = argparse.ArgumentParser(description='三条均线组合回归回测（A股/港股）')
    parser.add_argument('symbol', help='证券代码，A 股 6 位、港股 5 位')
    parser.add_argument('--market', default='auto', choices=['auto', 'A', 'HK'], help='市场')
    parser.add_argument('--start', default='2020-01-01', help='开始日期')
    parser.add_argument('--end', default=pd.Timestamp.today().strftime('%Y-%m-%d'), help='结束日期')
    parser.add_argument('--adjust', default='qfq', choices=['qfq', 'hfq', ''], help='复权方式')
    parser.add_argument('--ma', nargs=3, type=int, default=[5, 20, 60], help='三条均线周期')
    parser.add_argument('--ma-type', default='SMA', choices=['SMA', 'EMA'], help='均线类型')
    parser.add_argument('--buy', default='0>1,1<2', help='买入条件，如 0>1,1<2')
    parser.add_argument('--sell', default='0<1,1>2', help='卖出条件，如 0<1,1>2')
    parser.add_argument('--cash', type=float, default=10000.0, help='每次买入金额')
    parser.add_argument('--lot', type=int, default=100, help='每手股数')
    parser.add_argument('--commission', type=float, default=0.0005, help='佣金率（双边）')
    parser.add_argument('--buy-step', type=float, default=0.0,
                        help='买入间隔：条件成立后价格较基准变动百分之几才买入，0 表示条件成立即买入')
    parser.add_argument('--buy-mode', default='fall', choices=list(BUY_MODES),
                        help='买入触发方式：fall=较基准下跌，rise=较基准上涨')
    parser.add_argument('--sell-step', type=float, default=0.0,
                        help='卖出间隔：条件成立后价格较基准变动百分之几才减仓，0 表示条件成立即清仓')
    parser.add_argument('--sell-fraction', type=float, default=1.0,
                        help='每次卖出占持仓的比例，如 0.25 / 0.3333 / 0.5 / 1')
    parser.add_argument('--sell-mode', default='rise', choices=list(SELL_MODES),
                        help='分批卖出触发方式：rise=较上次卖价上涨，high=较持仓高点回落')
    parser.add_argument('--signal-mode', default='step', choices=list(SIGNAL_MODES),
                        help='成交时机：step=条件成立后等涨跌幅达标成交，'
                             'immediate=条件成立立即成交')
    parser.add_argument('--no-cache', action='store_true', help='不使用本地缓存')
    args = parser.parse_args(argv)

    try:
        df, code, market = fetch_daily(args.symbol, args.start, args.end, market=args.market,
                                       adjust=args.adjust, use_cache=not args.no_cache)
        result = run_backtest(
            df,
            ma_periods=args.ma,
            ma_type=args.ma_type,
            buy_conditions=parse_conditions(args.buy),
            sell_conditions=parse_conditions(args.sell),
            cash_per_trade=args.cash,
            lot_size=args.lot,
            commission=args.commission,
            symbol=code,
            market=market,
            step_buy_pct=args.buy_step,
            step_sell_pct=args.sell_step,
            sell_fraction=args.sell_fraction,
            sell_mode=args.sell_mode,
            buy_mode=args.buy_mode,
            signal_mode=args.signal_mode,
        )
    except Exception as exc:
        print('回测失败：%s' % exc)
        return 1

    print('%s（%s）%s ~ %s，共 %d 根K线' % (
        result['symbol'], result['market_name'],
        result['start'].strftime('%Y-%m-%d'), result['end'].strftime('%Y-%m-%d'), result['bars']))
    print('买入条件：%s；卖出条件：%s' % (
        describe_conditions(result['buy_conditions'], result['ma_periods']),
        describe_conditions(result['sell_conditions'], result['ma_periods'])))
    buy_batch, sell_batch = describe_batch(result['step_buy_pct'], result['step_sell_pct'],
                                           result['sell_fraction'], result['sell_mode'],
                                           result['buy_mode'], result['signal_mode'])
    print('分批设置：买入 %s；卖出 %s' % (buy_batch, sell_batch))
    print('总盈亏：%.2f 元（已实现 %.2f，浮动 %.2f），手续费合计 %.2f' % (
        result['total_pnl'], result['realized_pnl'], result['unrealized_pnl'],
        result['commission_total']))
    print('买入 %d 次，卖出 %d 次，已平仓 %d 笔，胜率 %.2f%%' % (
        result['buy_count'], result['sell_count'], result['closed_count'], result['win_rate']))
    if result['failed_buy_count']:
        print('买入失败 %d 次（条件成立但未能成交）：' % result['failed_buy_count'])
        for item in result['failed_buys'][:20]:
            print('  %s @ %.3f 未买入：%s' % (
                item['time'].strftime('%Y-%m-%d'), item['price'], item['reason']))
        if result['failed_buy_count'] > 20:
            print('  ...其余 %d 条未列出' % (result['failed_buy_count'] - 20))

    for item in result['trades']:
        print('买入 %s @ %.3f x %d -> 卖出 %s @ %.3f，盈亏 %.2f（%.2f%%）' % (
            item['buy_time'].strftime('%Y-%m-%d'), item['buy_price'], item['size'],
            item['sell_time'].strftime('%Y-%m-%d'), item['sell_price'],
            item['pnl'], item['pnl_pct']))

    if result['open_position']:
        pos = result['open_position']
        print('未平仓：买入 %s @ %.3f x %d，最新价 %.3f，浮动盈亏 %.2f' % (
            pos['buy_time'].strftime('%Y-%m-%d'), pos['buy_price'], pos['size'],
            pos['last_price'], pos['pnl']))
    return 0


if __name__ == '__main__':
    sys.exit(main())
