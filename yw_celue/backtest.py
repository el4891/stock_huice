"""单均线回撤策略回测引擎：组织 backtrader 运行并汇总盈亏、成交次数与买卖时点。

命令行示例：
    python backtest.py 600519 --start 2020-01-01 --end 2024-12-31 \\
        --ma 20 --position above --buy-drop 5 --sell-drop 8 --cash 10000
"""

import argparse
import sys

import backtrader as bt
import pandas as pd

from data_fetch import fetch_daily, market_name
from yw_strategy import YwStrategy

# 价格条件：above=收盘价在均线之上，below=收盘价在均线之下
POSITION_MODES = ('above', 'below')

# 成交时机：immediate=条件成立即成交，step=条件成立只设基准、等下跌达标
SIGNAL_MODES = ('immediate', 'step')

# 初始资金下限：倍数设得过小时兜底，避免连续加仓时资金不足被拒单
MIN_INITIAL_CASH = 100000.0


def run_backtest(df, ma_period=20, ma_type='SMA', position='above',
                 buy_drop_pct=5.0, sell_drop_pct=8.0, sell_rise_pct=8.0,
                 sell_fraction=1.0,
                 cash_per_trade=10000.0, initial_cash_multiple=20.0,
                 lot_size=100, commission=0.0005,
                 symbol='', market='A', signal_mode='step'):
    """执行回测并返回结果字典。

    df 需包含 datetime/open/high/low/close/volume 六列。
    initial_cash_multiple 为初始资金相对「每次买入金额」的倍数（须大于 0）；
    position 为买入的价格条件：above=价格在均线之上，below=价格在均线之下；
    buy_drop_pct 为较买入基准下跌的百分比，0 表示条件成立即买入；
    sell_drop_pct 为卖出回落百分比（须大于 0）：价格需先涨过「最后买入价 ×
    (1 + sell_drop_pct%)」才启动最高价跟踪，之后从最高价回落同比例时全部卖出；
    signal_mode 为成交时机：immediate=条件成立立即成交，step=条件成立只设基准、
    价格相对基准下跌达到 buy_drop_pct% 后才成交。
    """
    if df is None or df.empty:
        raise ValueError('行情数据为空，无法回测')

    ma_period = int(ma_period)
    if ma_period <= 0:
        raise ValueError('均线周期必须为正整数')

    position = str(position or 'above').lower()
    if position not in POSITION_MODES:
        raise ValueError('价格条件只能是 above（均线之上）或 below（均线之下）：%s' % position)

    cash_per_trade = float(cash_per_trade)
    if cash_per_trade <= 0:
        raise ValueError('每次买入金额必须大于 0')
    initial_cash_multiple = float(initial_cash_multiple)
    if initial_cash_multiple <= 0:
        raise ValueError('初始资金倍数必须大于 0')
    lot_size = max(int(lot_size), 1)
    commission = max(float(commission), 0.0)

    buy_drop_pct = max(float(buy_drop_pct), 0.0)
    sell_drop_pct = float(sell_drop_pct)
    if sell_drop_pct <= 0:
        raise ValueError('卖出回落幅度必须大于 0（否则没有卖出信号）')
    sell_rise_pct = float(sell_rise_pct)
    if sell_rise_pct < 0:
        raise ValueError('卖出启动涨幅不能为负数')
    sell_fraction = float(sell_fraction)
    if not 0.0 < sell_fraction <= 1.0:
        raise ValueError('每次卖出比例必须在 0 ~ 1 之间')

    signal_mode = str(signal_mode or 'step').lower()
    if signal_mode not in SIGNAL_MODES:
        raise ValueError('成交时机只能是 immediate（条件成立即成交）或 step（等下跌达标成交）：%s'
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
    if len(frame) <= ma_period:
        # 数据不足均线周期时 backtrader 内部会抛 IndexError，这里提前给出可读提示
        raise ValueError('行情数据仅 %d 根，不足以计算 MA%d，请扩大回测区间或减小均线周期'
                         % (len(frame), ma_period))

    feed = bt.feeds.PandasData(dataname=frame[['open', 'high', 'low', 'close', 'volume']])

    # 持仓期间下跌达标会持续加仓，初始资金按「每次买入金额 × 倍数」预留，
    # 倍数由资金设置界面指定；不足下限时按 MIN_INITIAL_CASH 兜底
    initial_cash = max(cash_per_trade * initial_cash_multiple, MIN_INITIAL_CASH)

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.adddata(feed, name=symbol or 'data')
    cerebro.addstrategy(
        YwStrategy,
        ma_period=ma_period,
        ma_type=ma_type,
        position=position,
        buy_drop_pct=buy_drop_pct,
        sell_drop_pct=sell_drop_pct,
        sell_rise_pct=sell_rise_pct,
        sell_fraction=sell_fraction,
        cash_per_trade=cash_per_trade,
        lot_size=lot_size,
        commission=commission,
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
        ma_period=ma_period,
        ma_type=ma_type,
        position=position,
        buy_drop_pct=buy_drop_pct,
        sell_drop_pct=sell_drop_pct,
        sell_rise_pct=sell_rise_pct,
        sell_fraction=sell_fraction,
        cash_per_trade=cash_per_trade,
        initial_cash_multiple=initial_cash_multiple,
        lot_size=lot_size,
        commission=commission,
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


def position_text(position):
    """价格条件转中文说明。"""
    return '价格在均线之下' if str(position) == 'below' else '价格在均线之上'


def signal_mode_text(signal_mode):
    """成交时机转中文说明。"""
    return '条件成立立即成交' if str(signal_mode) == 'immediate' else '成立后只设基准价格'


def fraction_text(fraction):
    """把每次卖出比例转成 1/8、1/4、1/2、全部 这样的文本。"""
    value = float(fraction)
    for candidate, text in ((1.0, '全部'), (0.5, '1/2'), (0.25, '1/4'), (0.125, '1/8')):
        if abs(value - candidate) < 1e-6:
            return text
    return '%.2f%%' % (value * 100)


def describe_rules(ma_period, position, buy_drop_pct, sell_drop_pct, signal_mode,
                   cash_per_trade, ma_type='SMA', sell_fraction=1.0, sell_rise_pct=8.0):
    """把策略参数转成可读文本，返回 (买入说明, 卖出说明)。"""
    ma_name = '%s%d' % (str(ma_type).upper(), int(ma_period))
    condition = '价格在 %s %s' % (ma_name, '之下' if str(position) == 'below' else '之上')
    cash_text = '每次买入 %.0f 元' % float(cash_per_trade)

    if str(signal_mode) == 'immediate':
        if float(buy_drop_pct) > 0:
            buy_text = ('%s时立即买入一次，之后每再下跌 %.2f%% 加仓一次，%s'
                        % (condition, float(buy_drop_pct), cash_text))
        else:
            buy_text = '%s时立即买入，%s（不限制下跌幅度）' % (condition, cash_text)
    else:
        if float(buy_drop_pct) > 0:
            buy_text = ('%s时以当时收盘价为基准，再下跌 %.2f%% 买入一次，%s'
                        % (condition, float(buy_drop_pct), cash_text))
        else:
            buy_text = '%s时以当时收盘价为基准直接买入，%s' % (condition, cash_text)

    fraction_label = fraction_text(sell_fraction)
    if fraction_label == '全部':
        volume_text = '全部卖出'
    else:
        volume_text = '卖出持仓的 %s（剩余不足一手时全部卖出）' % fraction_label
    sell_text = ('以最近一次买入或卖出成交价为基准，上涨超过 %.2f%% 后开始记录最高价，'
                 '从最高价回落 %.2f%% 时%s'
                 % (float(sell_rise_pct), float(sell_drop_pct), volume_text))
    return buy_text, sell_text


def main(argv=None):
    parser = argparse.ArgumentParser(description='单均线回撤策略回测（A股/港股）')
    parser.add_argument('symbol', help='证券代码，A 股 6 位、港股 5 位')
    parser.add_argument('--market', default='auto', choices=['auto', 'A', 'HK'], help='市场')
    parser.add_argument('--start', default='2020-01-01', help='开始日期')
    parser.add_argument('--end', default=pd.Timestamp.today().strftime('%Y-%m-%d'), help='结束日期')
    parser.add_argument('--adjust', default='qfq', choices=['qfq', 'hfq', ''], help='复权方式')
    parser.add_argument('--ma', type=int, default=20, help='均线周期')
    parser.add_argument('--ma-type', default='SMA', choices=['SMA', 'EMA'], help='均线类型')
    parser.add_argument('--position', default='above', choices=list(POSITION_MODES),
                        help='价格条件：above=价格在均线之上，below=价格在均线之下')
    parser.add_argument('--buy-drop', type=float, default=5.0,
                        help='较买入基准下跌百分之几买入，0 表示条件成立即买入')
    parser.add_argument('--sell-drop', type=float, default=8.0,
                        help='从最高价回落百分之几卖出')
    parser.add_argument('--sell-rise', type=float, default=8.0,
                        help='相对观察基准上涨百分之几后开始记录最高价')
    parser.add_argument('--sell-fraction', type=float, default=1.0,
                        help='每次卖出占持仓的比例，如 0.125 / 0.25 / 0.5 / 1')
    parser.add_argument('--cash', type=float, default=100000.0, help='每次买入金额')
    parser.add_argument('--cash-multiple', type=float, default=20.0,
                        help='初始资金倍数（初始资金 = 每次买入金额 × 倍数）')
    parser.add_argument('--lot', type=int, default=100, help='每手股数')
    parser.add_argument('--commission', type=float, default=0.0005, help='佣金率（双边）')
    parser.add_argument('--signal-mode', default='step', choices=list(SIGNAL_MODES),
                        help='成交时机：step=条件成立后等下跌达标，immediate=条件成立立即成交')
    parser.add_argument('--no-cache', action='store_true', help='不使用本地缓存')
    args = parser.parse_args(argv)

    try:
        df, code, market = fetch_daily(args.symbol, args.start, args.end, market=args.market,
                                       adjust=args.adjust, use_cache=not args.no_cache)
        result = run_backtest(
            df,
            ma_period=args.ma,
            ma_type=args.ma_type,
            position=args.position,
            buy_drop_pct=args.buy_drop,
            sell_drop_pct=args.sell_drop,
            sell_rise_pct=args.sell_rise,
            sell_fraction=args.sell_fraction,
            cash_per_trade=args.cash,
            initial_cash_multiple=args.cash_multiple,
            lot_size=args.lot,
            commission=args.commission,
            symbol=code,
            market=market,
            signal_mode=args.signal_mode,
        )
    except Exception as exc:
        print('回测失败：%s' % exc)
        return 1

    print('%s（%s）%s ~ %s，共 %d 根K线' % (
        result['symbol'], result['market_name'],
        result['start'].strftime('%Y-%m-%d'), result['end'].strftime('%Y-%m-%d'), result['bars']))
    buy_rule, sell_rule = describe_rules(result['ma_period'], result['position'],
                                         result['buy_drop_pct'], result['sell_drop_pct'],
                                         result['signal_mode'], result['cash_per_trade'],
                                         result['ma_type'], result['sell_fraction'],
                                         result['sell_rise_pct'])
    print('成交时机：%s' % signal_mode_text(result['signal_mode']))
    print('买入规则：%s' % buy_rule)
    print('卖出规则：%s' % sell_rule)
    print('初始资金：%.2f 元（每次买入 %.2f 元 × %.1f 倍）' % (
        result['initial_cash'], result['cash_per_trade'], result['initial_cash_multiple']))
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
