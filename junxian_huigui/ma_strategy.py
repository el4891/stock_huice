"""三条均线自由组合的回归策略（backtrader）。

信号规则：
    买入条件、卖出条件均由若干「均线A 比较符 均线B」条件组成，条件之间取「与」关系。
    例如 ((0, '>', 1), (1, '<', 2)) 表示 MA1 > MA2 且 MA2 < MA3。

交易规则：
    满足买入条件 -> 按「每次买入金额」买入整手，空仓或持仓均适用；
    持仓且满足卖出条件 -> 按「每次卖出比例」减仓（默认全部卖出）；
    分批买入：持仓期间买入条件仍成立时，按 buy_mode 判断是否加仓——fall 为价格较上一次
              买入成交价下跌 step_buy_pct%，rise 为价格较上一次买入成交价上涨
              step_buy_pct%；step_buy_pct 为 0 表示不限制价格间隔；
              买入条件由成立转为不成立时清空买入基准，条件重新成立时立即买入；
    分批卖出：持仓期间卖出条件仍成立，按 sell_mode 判断是否减仓——rise 为价格较上一次
              卖出成交价上涨 step_sell_pct%，high 为价格较持仓期间最高价回落
              step_sell_pct%；step_sell_pct 为 0 时信号成立即一次性清仓；
              卖出条件由成立转为不成立时清空卖出基准，下一次条件成立即卖出；
    持仓成本按加权平均价结转，部分卖出只结转对应比例的成本；
    回测结束时未平仓的持仓不计入已实现盈亏，单独记录为浮动盈亏。
"""

import backtrader as bt


class ThreeMaStrategy(bt.Strategy):
    params = dict(
        ma_periods=(5, 20, 60),   # 三条均线周期
        ma_type='SMA',            # SMA / EMA
        buy_conditions=(),        # 买入条件元组，元素为 (左均线索引, '>' 或 '<', 右均线索引)
        sell_conditions=(),       # 卖出条件元组
        cash_per_trade=10000.0,   # 每次买入金额（元）
        lot_size=100,             # 每手股数（A 股 100，港股按实际每手）
        commission=0.0005,        # 佣金率（双边，含税费）
        step_buy_pct=0.0,         # 分批买入的间隔（百分比），0 表示条件成立即买入
        step_sell_pct=0.0,        # 分批卖出的间隔（百分比），0 表示一次性清仓
        sell_fraction=1.0,        # 每次卖出占持仓的比例（1/4、1/3、1/2、1）
        sell_mode='rise',         # 卖出触发方式：rise=最近卖价上涨，high=持仓高点回落
        buy_mode='fall',          # 买入触发方式：fall=较上次买价下跌，rise=较上次买价上涨
    )

    def __init__(self):
        self.mas = [self._create_ma(period) for period in self.p.ma_periods]
        self.pending_order = None
        self.trades = []           # 已平仓交易明细
        self.order_log = []        # 成交委托记录
        self.open_position = None  # 回测结束时的未平仓持仓
        self.messages = []         # 运行日志（界面展示）
        self.skipped_count = 0     # 因买入金额不足一手而跳过的信号次数

        # 当前持仓的加权成本：amount 为买入金额合计（不含佣金），comm 为对应买入佣金
        self.position_size = 0
        self.position_amount = 0.0
        self.position_comm = 0.0
        self.position_time = None  # 本轮持仓首次买入时间
        self.buy_anchor = None     # 上一次买入成交价（分批买入的下跌基准）
        self.sell_anchor = None    # 上一次卖出成交价（分批卖出的上涨基准）
        self.peak_price = None     # 持仓期间最高价（从高点回落卖出的基准）
        self.buy_signal_active = False   # 上一根 K 线买入条件是否成立
        self.buy_reset = False           # 买入条件失效后基准已清空，下次成立即买入
        self.sell_signal_active = False  # 上一根 K 线卖出条件是否成立
        self.sell_reset = False          # 卖出条件失效后基准已清空，下次成立即卖出
        # 回测结束时的基准状态（stop 中写入，界面展示用）
        self.anchor_state = dict(buy_anchor=None, sell_anchor=None, peak_price=None,
                                 buy_reset=False, sell_reset=False)

    def _create_ma(self, period):
        if str(self.p.ma_type).upper() == 'EMA':
            return bt.indicators.ExponentialMovingAverage(self.data.close, period=period)
        return bt.indicators.SimpleMovingAverage(self.data.close, period=period)

    def _log(self, text):
        # 限制日志条数，避免长时间回测占用过多内存
        if len(self.messages) < 500:
            self.messages.append(text)

    def _match(self, conditions):
        """判断条件组是否全部成立。"""
        for left, op, right in conditions:
            left_value = self.mas[int(left)][0]
            right_value = self.mas[int(right)][0]
            if op == '>':
                if not left_value > right_value:
                    return False
            else:
                if not left_value < right_value:
                    return False
        return True

    def _calc_size(self, price):
        """按固定金额计算可买整手数量。"""
        lot = max(int(self.p.lot_size), 1)
        return int(self.p.cash_per_trade / price / lot) * lot

    def _avg_price(self):
        """当前持仓的加权平均买入价（不含佣金）。"""
        if self.position_size <= 0:
            return 0.0
        return self.position_amount / self.position_size

    def _reset_position(self):
        """全部卖出后清空持仓成本记录。"""
        self.position_size = 0
        self.position_amount = 0.0
        self.position_comm = 0.0
        self.position_time = None
        self.buy_anchor = None
        self.sell_anchor = None
        self.peak_price = None
        self.buy_reset = False
        self.sell_reset = False

    def _can_add_buy(self, price):
        """持仓期间加仓：基准已清空或间隔为 0 时，买入条件成立即可继续买入。"""
        if self.buy_reset or self.buy_anchor is None:
            return True
        step = float(self.p.step_buy_pct)
        if step <= 0:
            return True
        if str(self.p.buy_mode) == 'rise':
            # 追涨加仓：较上一次买入价上涨 step_buy_pct% 时再加仓
            return price >= self.buy_anchor * (1 + step / 100.0)
        return price <= self.buy_anchor * (1 - step / 100.0)

    def _track_signals(self):
        """跟踪买卖条件：条件由成立转为不成立时清空对应基准，下次成立即成交。"""
        buy_matched = bool(self.p.buy_conditions) and self._match(self.p.buy_conditions)
        if self.buy_signal_active and not buy_matched:
            self.buy_anchor = None
            self.buy_reset = True
        self.buy_signal_active = buy_matched

        sell_matched = bool(self.p.sell_conditions) and self._match(self.p.sell_conditions)
        if self.sell_signal_active and not sell_matched:
            self.sell_anchor = None
            self.peak_price = None
            self.sell_reset = True
        self.sell_signal_active = sell_matched

    def _update_peak(self):
        """记录持仓期间的最高价，作为「从高点回落」卖出的基准。"""
        if self.sell_reset:
            # 基准已被清空，等待下一次卖出条件成立后重新建立
            return
        high = self.data.high[0]
        if self.peak_price is None or high > self.peak_price:
            self.peak_price = high

    def _can_sell(self, price):
        """分批卖出：按触发方式判断本次是否允许减仓。

        rise：较上一次卖出价上涨 step_sell_pct% 时减仓；
        high：较持仓期间最高价回落 step_sell_pct% 时减仓。
        卖出条件失效后基准会被清空，条件再次成立时直接卖出；
        间隔为 0 表示不分批，信号成立即一次性清仓。
        """
        step = float(self.p.step_sell_pct)
        if step <= 0 or self.sell_reset:
            return True

        if str(self.p.sell_mode) == 'high':
            if self.peak_price is None:
                return True
            return price <= self.peak_price * (1 - step / 100.0)

        if self.sell_anchor is None:
            return True   # 本轮首次卖出，无需等待上涨
        return price >= self.sell_anchor * (1 + step / 100.0)

    def _sell_size(self):
        """按「每次卖出比例」计算本次卖出手数，卖出零股时一次性清仓。"""
        lot = max(int(self.p.lot_size), 1)
        fraction = float(self.p.sell_fraction)
        if fraction >= 1.0:
            return self.position_size
        size = int(self.position_size * fraction / lot) * lot
        if size <= 0:
            # 剩余持仓不足一手，无法再拆分，直接全部卖出
            return self.position_size
        return min(size, self.position_size)

    def _submit_buy(self, price):
        size = self._calc_size(price)
        if size <= 0:
            self.skipped_count += 1
            self._log('%s 买入金额不足以买入 1 手（收盘价 %.2f，每手 %d 股）'
                      % (self.data.datetime.date(0), price, int(self.p.lot_size)))
            return
        self.pending_order = self.buy(size=size)

    def _submit_sell(self):
        size = self._sell_size()
        if size <= 0:
            return
        self.pending_order = self.sell(size=size)

    def next(self):
        # 先跟踪买卖条件状态，条件失效时清空对应基准
        self._track_signals()

        # 持仓期间持续跟踪最高价，即使当日有挂单也需要记录
        if self.position_size > 0:
            self._update_peak()

        if self.pending_order is not None:
            return

        price = self.data.close[0]
        if price <= 0:
            return

        if self.position_size <= 0:
            if not self.p.buy_conditions or not self._match(self.p.buy_conditions):
                return
            self._submit_buy(price)
            return

        # 持仓中：卖出信号优先，避免同一天既加仓又减仓
        if self.p.sell_conditions and self._match(self.p.sell_conditions):
            if self._can_sell(price):
                self._submit_sell()
            return

        if self.p.buy_conditions and self._match(self.p.buy_conditions):
            if self._can_add_buy(price):
                self._submit_buy(price)

    def notify_order(self, order):
        if order.status in (order.Submitted, order.Accepted):
            return

        if order.status == order.Completed:
            self.pending_order = None
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
                self.buy_reset = False
                self.order_log.append(dict(side='buy', time=trade_time, price=price,
                                           size=size, amount=amount, comm=comm,
                                           pnl=None, pnl_pct=None,
                                           position_after=self.position_size))
                return

            # 卖出：按加权平均成本结转本次卖出对应比例的成本
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
                                       position_after=self.position_size - size))

            self.position_size -= size
            self.position_amount -= sold_amount
            self.position_comm -= sold_comm
            if self.position_size <= 0:
                self._reset_position()
            else:
                # 部分卖出后把卖出价作为新的基准，避免价格继续下跌时连续触发
                self.sell_anchor = price
                self.peak_price = price
                self.sell_reset = False
            return

        # 订单被拒绝、取消等情况：释放挂单状态并记录原因
        self.pending_order = None
        self._log('%s 委托未成交：%s' % (self.data.datetime.date(0), order.getstatusname()))

    def stop(self):
        # 记录回测结束时的基准状态，供界面展示
        self.anchor_state = dict(
            buy_anchor=self.buy_anchor,
            sell_anchor=self.sell_anchor,
            peak_price=self.peak_price,
            buy_reset=self.buy_reset,
            sell_reset=self.sell_reset,
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
