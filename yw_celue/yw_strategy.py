"""单均线回撤策略（backtrader）。

交易规则：
    价格条件：收盘价在均线之上（position='above'）或之下（position='below'）。
    买入：价格条件成立后，价格较基准下跌 buy_drop_pct% 时按「每次买入金额」买入整手。
        持仓期间未触发卖出时同样按基准下跌幅度加仓（卖出后按卖出价基准买回）；
        signal_mode='immediate'：空仓时条件成立即买入一次，之后每再下跌 buy_drop_pct% 加仓；
        signal_mode='step'：条件成立时只记录基准价格（当时收盘价），下跌达标后才买入；
        buy_drop_pct=0 表示不限制下跌幅度（条件成立即买入）。
        成交后基准更新为成交价；条件失效时清空基准，重新成立时重建。
    卖出：以「卖出观察基准价」为门槛，价格（收盘价）先涨过「基准价 ×
        (1 + sell_rise_pct%)」才启动最高价跟踪（含启动当根K线的最高价），
        之后收盘价从最高价回落 sell_drop_pct% 时按 sell_fraction 比例卖出
        （1/8、1/4、1/2 或全部，按当前持仓计算，剩余不足一手时一次性卖完）；
        加仓成交后观察基准切换为最后买入价并重置最高点；
        每次卖出成交后观察基准切换为卖出成交价，重新等待涨过新门槛再跟踪。
    卖出后的买入：卖出成交价作为新的买入基准，价格条件成立且较该基准再下跌
        buy_drop_pct% 时买入（立即成交模式同样等待下跌）；该基准不随上涨上移，
        直到买入成交或价格条件失效。
    回测结束时未平仓的持仓不计入已实现盈亏，单独记录为浮动盈亏。
"""

import backtrader as bt

# 委托未成交的状态名 -> 中文原因（用于买入失败明细）
FAIL_REASONS = {
    'Margin': '可用资金不足',
    'Rejected': '委托被拒绝',
    'Canceled': '委托已取消',
    'Expired': '委托已过期',
}


class YwStrategy(bt.Strategy):
    params = dict(
        ma_period=20,             # 均线周期
        ma_type='SMA',            # SMA / EMA
        position='above',         # 价格条件：above=在均线之上，below=在均线之下
        buy_drop_pct=5.0,         # 较基准下跌百分之几买入
        sell_drop_pct=8.0,        # 从最高价回落百分之几卖出
        sell_rise_pct=8.0,        # 基准上涨百分之几后开始记录最高价
        sell_fraction=1.0,        # 每次卖出占持仓的比例（1/8、1/4、1/2、1）
        cash_per_trade=10000.0,   # 每次买入金额（元）
        lot_size=100,             # 每手股数（A 股 100，港股按实际每手）
        commission=0.0005,        # 佣金率（双边，含税费）
        signal_mode='step',       # 成交时机：step=等下跌达标，immediate=条件成立即成交
    )

    def __init__(self):
        self.ma = self._create_ma(self.p.ma_period)
        self.pending_order = None
        self.trades = []           # 已平仓交易明细
        self.order_log = []        # 成交委托记录
        self.open_position = None  # 回测结束时的未平仓持仓
        self.messages = []         # 运行日志（界面展示）
        self.skipped_count = 0     # 因买入金额不足一手而跳过的信号次数
        self.failed_buys = []      # 买入失败明细（时间、信号价、原因），界面展示用
        self.failed_buy_count = 0  # 买入失败总次数（明细最多保留 500 条）
        self.pending_price = None  # 挂单对应的信号价（成交失败时记录用）

        # 当前持仓的加权成本：amount 为买入金额合计（不含佣金），comm 为对应买入佣金
        self.position_size = 0
        self.position_amount = 0.0
        self.position_comm = 0.0
        self.position_time = None  # 本轮持仓首次买入时间
        self.buy_anchor = None     # 买入基准：条件成立时的收盘价、卖出价或上一次买入成交价
        self.buy_anchor_fixed = False  # 买入基准是否为卖出价（固定不随新高上移）
        self.last_buy_price = None # 最后一次买入成交价
        self.sell_base = None      # 卖出观察基准价：最后买入成交价或最近卖出成交价
        self.peak_price = None     # 启动卖出观察后的最高价（回落卖出的基准）
        self.sell_watch = False    # 是否已启动卖出观察（价格涨过观察基准门槛）
        self.buy_active = False    # 上一根 K 线价格条件是否成立
        self.buy_reset = False     # 条件失效后基准已清空（界面展示用）
        # 回测结束时的基准状态（stop 中写入，界面展示用）
        self.anchor_state = dict(buy_anchor=None, last_buy_price=None, sell_base=None,
                                 peak_price=None, sell_watch=False, buy_reset=False,
                                 buy_anchor_fixed=False)

    def _create_ma(self, period):
        if str(self.p.ma_type).upper() == 'EMA':
            return bt.indicators.ExponentialMovingAverage(self.data.close, period=period)
        return bt.indicators.SimpleMovingAverage(self.data.close, period=period)

    def _log(self, text):
        # 限制日志条数，避免长时间回测占用过多内存
        if len(self.messages) < 500:
            self.messages.append(text)

    def _match(self):
        """判断价格条件是否成立：收盘价在均线之上 / 之下。"""
        value = self.ma[0]
        if value != value:  # 均线数据不足时为 NaN，视为条件不成立
            return False
        if str(self.p.position) == 'below':
            return self.data.close[0] < value
        return self.data.close[0] > value

    def _step_mode(self):
        """是否采用「条件成立只设基准、等下跌达标」的成交时机。"""
        return str(self.p.signal_mode) != 'immediate'

    def _calc_size(self, price):
        """按固定金额计算可买整手数量。"""
        lot = max(int(self.p.lot_size), 1)
        return int(self.p.cash_per_trade / price / lot) * lot

    def _record_buy_failure(self, price, reason, size=0):
        """记录一次买入失败：时间、信号价、原计划数量与原因（明细最多保留 500 条）。"""
        self.failed_buy_count += 1
        if len(self.failed_buys) >= 500:
            return
        self.failed_buys.append(dict(
            time=self.data.datetime.datetime(0),
            price=float(price),
            size=int(size),
            reason=reason,
            position=self.position_size,
        ))

    def _avg_price(self):
        """当前持仓的加权平均买入价（不含佣金）。"""
        if self.position_size <= 0:
            return 0.0
        return self.position_amount / self.position_size

    def _reset_position(self):
        """清空持仓成本与卖出观察状态（全部卖出后调用）。"""
        self.position_size = 0
        self.position_amount = 0.0
        self.position_comm = 0.0
        self.position_time = None
        self.buy_anchor = None
        self.buy_anchor_fixed = False
        self.last_buy_price = None
        self.sell_base = None
        self.peak_price = None
        self.sell_watch = False
        self.buy_reset = False

    def _can_buy(self, price):
        """判断本次是否允许买入：基准为空或下跌幅度要求为 0 时直接放行。"""
        step = max(float(self.p.buy_drop_pct), 0.0)
        if step <= 0 or self.buy_anchor is None:
            return True
        return price <= self.buy_anchor * (1 - step / 100.0)

    def _track_condition(self):
        """跟踪价格条件，返回当日条件是否成立。

        条件刚成立时：等下跌方式记录基准（当时收盘价），立即成交方式按空基准放行买入；
        条件持续成立且空仓时：基准跟踪期间最高收盘价，回撤达标才买入；
        卖出后固定为卖出价的基准不参与跟踪，避免基准被上涨抬高；
        条件由成立转为不成立时清空基准，等待重新成立。
        """
        matched = self._match()
        if matched:
            if not self.buy_active:
                self.buy_reset = False
                # 等下跌方式，或已持仓的立即成交方式：先只记录基准，
                # 买入/加仓动作等下跌幅度达标（持仓中不重复立即加仓）
                if self._step_mode() or self.position_size > 0:
                    self.buy_anchor = self.data.close[0]
                    self.buy_anchor_fixed = False
            elif (self._step_mode() and self.position_size == 0
                  and self.buy_anchor is not None and not self.buy_anchor_fixed):
                # 空仓等待期间跟随新高，保证持续上涨后回撤也能触发买入
                self.buy_anchor = max(self.buy_anchor, self.data.close[0])
        elif self.buy_anchor is not None or self.buy_active:
            # 条件失效：清空基准。卖出成交在次日开盘，卖出价基准可能在条件
            # 已经失效之后才建立，因此这里不能只看上一根K线的状态
            self.buy_anchor = None
            self.buy_anchor_fixed = False
            self.buy_reset = True
        self.buy_active = matched
        return matched

    def _update_peak(self):
        """记录卖出观察期间的最高价（含启动当根K线的最高价）。"""
        high = self.data.high[0]
        if self.peak_price is None or high > self.peak_price:
            self.peak_price = high

    def _sell_triggered(self, price):
        """判断是否触发卖出：先过启动门槛，再按最高点回落判断。

        价格为收盘价；启动门槛为「观察基准价 × (1 + sell_rise_pct%)」，基准为
        最近一次买入成交价（加仓后）或最近一次卖出成交价（卖出后）；
        未达标前不记录最高点，达标后把当天最高价作为高点起点继续跟踪，
        之后收盘价从最高价回落 sell_drop_pct% 时触发卖出。
        """
        drop = max(float(self.p.sell_drop_pct), 0.0)
        rise = max(float(self.p.sell_rise_pct), 0.0)
        if drop <= 0 or self.sell_base is None:
            return False

        if not self.sell_watch:
            if price <= self.sell_base * (1 + rise / 100.0):
                return False
            # 刚涨过启动门槛：开始记录最高点，当天最高价直接纳入跟踪
            self.sell_watch = True
            self.peak_price = self.data.high[0]

        self._update_peak()
        if self.peak_price is None:
            return False
        return price <= self.peak_price * (1 - drop / 100.0)

    def _submit_buy(self, price):
        size = self._calc_size(price)
        if size <= 0:
            self.skipped_count += 1
            self._record_buy_failure(price, '买入金额不足 1 手（可提高「每次买入金额」）')
            self._log('%s 买入金额不足以买入 1 手（收盘价 %.2f，每手 %d 股）'
                      % (self.data.datetime.date(0), price, int(self.p.lot_size)))
            return
        self.pending_price = price
        self.pending_order = self.buy(size=size)

    def _sell_size(self):
        """按「每次卖出比例」计算本次卖出手数，剩余不足一手时一次性清仓。"""
        if self.position_size <= 0:
            return 0
        fraction = float(self.p.sell_fraction)
        if fraction >= 1.0:
            return self.position_size
        lot = max(int(self.p.lot_size), 1)
        size = int(self.position_size * fraction / lot) * lot
        if size <= 0:
            # 剩余持仓不足一手，无法再拆分，直接全部卖出
            return self.position_size
        return min(size, self.position_size)

    def _submit_sell(self):
        size = self._sell_size()
        if size <= 0:
            return
        self.pending_price = self.data.close[0]
        self.pending_order = self.sell(size=size)

    def next(self):
        # 先跟踪价格条件：成立时记录基准，失效时清空基准
        matched = self._track_condition()

        if self.pending_order is not None:
            return

        price = self.data.close[0]
        if price <= 0:
            return

        # 持仓中：回落卖出优先，避免同一天既加仓又减仓；
        # 未触发卖出时仍可继续按基准下跌幅度加仓（卖出后跌到门槛也会买回）
        if self.position_size > 0 and self._sell_triggered(price):
            self._submit_sell()
            return

        if not matched:
            return

        if self._step_mode() and self.buy_anchor is None:
            # 开仓前条件就已成立（例如刚卖出），只重建基准，等下跌达标再买入
            self.buy_anchor = price
            self.buy_anchor_fixed = False
            self.buy_reset = False
            if float(self.p.buy_drop_pct) > 0:
                return
        if self._can_buy(price):
            self._submit_buy(price)

    def notify_order(self, order):
        if order.status in (order.Submitted, order.Accepted):
            return

        if order.status == order.Completed:
            self.pending_order = None
            signal_price = self.pending_price  # 触发委托当天的收盘价（信号价）
            self.pending_price = None
            price = order.executed.price
            size = abs(int(order.executed.size))
            amount = price * size
            comm = order.executed.comm
            trade_time = self.data.datetime.datetime(0)

            if order.isbuy():
                self.position_size += size
                self.position_amount += amount
                self.position_comm += comm
                if self.position_time is None:
                    self.position_time = trade_time
                self.buy_anchor = price
                self.buy_anchor_fixed = False
                self.buy_reset = False
                # 卖出观察基准切换为最后买入成交价；加仓后重置高点，等再涨过新门槛
                self.last_buy_price = price
                self.sell_base = price
                self.peak_price = None
                self.sell_watch = False
                self.order_log.append(dict(side='buy', time=trade_time, price=price,
                                           size=size, amount=amount, comm=comm,
                                           pnl=None, pnl_pct=None,
                                           signal_price=signal_price,
                                           position_after=self.position_size))
                return

            # 卖出：按本次卖出数量占持仓的比例结转成本
            share = (size / self.position_size) if self.position_size else 0.0
            sold_amount = self.position_amount * share
            sold_comm = self.position_comm * share
            total_cost = sold_amount + sold_comm
            pnl = amount - comm - total_cost
            pnl_pct = (pnl / total_cost * 100) if total_cost else 0.0
            self.trades.append(dict(
                buy_time=self.position_time,
                buy_price=(sold_amount / size) if size else 0.0,
                size=size,
                buy_amount=sold_amount,
                buy_comm=sold_comm,
                sell_time=trade_time,
                sell_price=price,
                sell_amount=amount,
                sell_comm=comm,
                pnl=pnl,
                pnl_pct=pnl_pct,
            ))
            self.order_log.append(dict(side='sell', time=trade_time, price=price,
                                       size=size, amount=amount, comm=comm,
                                       pnl=pnl, pnl_pct=pnl_pct,
                                       position_after=max(self.position_size - size, 0),
                                       signal_price=signal_price,
                                       sell_base=self.sell_base,
                                       peak_price=self.peak_price))
            self.position_size -= size
            self.position_amount -= sold_amount
            self.position_comm -= sold_comm

            # 卖出后：买入基准与卖出观察基准都切换为卖出成交价
            self.buy_anchor = price
            self.buy_anchor_fixed = True
            self.buy_reset = False
            self.sell_base = price
            self.peak_price = None
            self.sell_watch = False
            if self.position_size <= 0:
                # 全部卖出：清空持仓成本，基准仍按卖出价保留
                self._reset_position()
                self.buy_anchor = price
                self.buy_anchor_fixed = True
                self.sell_base = price
            return

        # 订单被拒绝、取消等情况：释放挂单状态并记录原因
        status_name = order.getstatusname()
        self.pending_order = None
        if order.isbuy():
            # 买入失败的委托计入明细，让结果中能看到「条件成立却没买入」的原因
            self._record_buy_failure(self.pending_price or self.data.close[0],
                                     FAIL_REASONS.get(status_name, status_name),
                                     abs(int(getattr(order.created, 'size', 0) or 0)))
        self.pending_price = None
        self._log('%s 委托未成交：%s' % (self.data.datetime.date(0), status_name))

    def stop(self):
        # 最后一根 K 线产生的委托没有次日开盘可成交，计入买入失败明细
        if self.pending_order is not None and self.pending_order.isbuy():
            self._record_buy_failure(self.pending_price or self.data.close[0],
                                     '回测结束时委托未成交')
        self.pending_order = None
        self.pending_price = None

        # 记录回测结束时的基准状态，供界面展示
        self.anchor_state = dict(
            buy_anchor=self.buy_anchor,
            buy_anchor_fixed=self.buy_anchor_fixed,
            last_buy_price=self.last_buy_price,
            sell_base=self.sell_base,
            peak_price=self.peak_price,
            sell_watch=self.sell_watch,
            buy_reset=self.buy_reset,
        )

        # 回测结束时仍持仓，按最后收盘价计算浮动盈亏，不记为已实现
        if self.position_size > 0:
            last_price = self.data.close[0]
            market_value = last_price * self.position_size
            total_cost = self.position_amount + self.position_comm
            pnl = market_value - total_cost
            pnl_pct = (pnl / total_cost * 100) if total_cost else 0.0
            self.open_position = dict(
                buy_time=self.position_time,
                buy_price=self._avg_price(),
                size=self.position_size,
                buy_amount=self.position_amount,
                buy_comm=self.position_comm,
                last_price=last_price,
                market_value=market_value,
                pnl=pnl,
                pnl_pct=pnl_pct,
            )
