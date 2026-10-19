"""均线回归回测工具的 Qt 图形界面。

功能：
    输入 A 股 / 港股代码，选择三条均线的周期与类型，自由组合「均线A 比较符 均线B」
    形式的买入、卖出条件，设置每次买入金额后回测，展示盈亏、买卖次数与买卖时点。
    支持分批买卖，成交时机可选：条件成立立即成交（旧方式），或条件成立时只更新基准价格、
    价格相对基准变动达到指定百分比才成交；卖出可按「每上涨」/「从高点回落」减仓
    1/4、1/3、1/2 或全部。

依赖：PyQt5（未安装时自动回退到 PySide6）、backtrader、akshare。
"""

import sys
from pathlib import Path

try:
    from PyQt5.QtCore import QDate, QSettings, QThread, Qt, pyqtSignal as Signal
    from PyQt5.QtGui import QColor, QFont, QPalette
    from PyQt5.QtWidgets import (
        QAbstractItemView,
        QApplication,
        QCheckBox,
        QComboBox,
        QDateEdit,
        QDoubleSpinBox,
        QFileDialog,
        QGroupBox,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMessageBox,
        QPlainTextEdit,
        QPushButton,
        QScrollArea,
        QSpinBox,
        QSplitter,
        QTableWidget,
        QTableWidgetItem,
        QVBoxLayout,
        QWidget,
    )

    QT_BINDING = 'PyQt5'
except ImportError:
    from PySide6.QtCore import QDate, QSettings, QThread, Qt, Signal
    from PySide6.QtGui import QColor, QFont, QPalette
    from PySide6.QtWidgets import (
        QAbstractItemView,
        QApplication,
        QCheckBox,
        QComboBox,
        QDateEdit,
        QDoubleSpinBox,
        QFileDialog,
        QGroupBox,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMessageBox,
        QPlainTextEdit,
        QPushButton,
        QScrollArea,
        QSpinBox,
        QSplitter,
        QTableWidget,
        QTableWidgetItem,
        QVBoxLayout,
        QWidget,
    )

    QT_BINDING = 'PySide6'

# 源码运行时程序目录即脚本目录；打包成 exe 后只读资源在临时解包目录（_MEIPASS），
# 而配置需要写入，因此统一放到 exe 所在目录，避免写入临时目录导致配置丢失
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).resolve().parent
    RES_DIR = Path(getattr(sys, '_MEIPASS', BASE_DIR))
else:
    BASE_DIR = Path(__file__).resolve().parent
    RES_DIR = BASE_DIR

sys.path.insert(0, str(BASE_DIR))

from backtest import (MIN_INITIAL_CASH, describe_batch, describe_conditions,
                      fraction_text, run_backtest)
from data_fetch import fetch_daily

ASSETS_DIR = RES_DIR / 'assets'
# 界面配置本地保存位置（与脚本/exe 同目录，ini 格式便于手工查看/删除）
SETTINGS_PATH = BASE_DIR / 'settings.ini'


def _asset_uri(name):
    """生成 QSS 可用的图片路径（统一使用正斜杠绝对路径）。"""
    return (ASSETS_DIR / name).as_posix()


def _ensure_default_settings():
    """首次以 exe 运行时，把内置的默认配置复制到 exe 目录，便于用户查看与修改。"""
    if SETTINGS_PATH.exists():
        return
    default_path = RES_DIR / 'settings.ini'
    if not default_path.exists() or default_path == SETTINGS_PATH:
        return
    try:
        SETTINGS_PATH.write_bytes(default_path.read_bytes())
    except OSError:
        # 无写入权限时忽略，界面仍会使用代码内的默认值
        pass


def _to_int(value, default):
    """把 ini 中的字符串安全转换为 int，异常时返回默认值。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value, default):
    """把 ini 中的字符串安全转换为 float，异常时返回默认值。"""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value, default):
    """兼容 ini 中 true/false、1/0 等写法，异常时返回默认值。"""
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in ('1', 'true', 'yes', 'on'):
        return True
    if text in ('0', 'false', 'no', 'off'):
        return False
    return default


ARROW_UP_URI = _asset_uri('arrow_up.png')
ARROW_DOWN_URI = _asset_uri('arrow_down.png')

STYLE_SHEET = """
QWidget { font-family: 'Microsoft YaHei UI', 'Segoe UI', sans-serif; font-size: 13px; }
#centralWidget { background: #0f172a; }
QLabel { background: transparent; color: #e5e7eb; }
QLabel#title { font-size: 20px; font-weight: 700; color: #f8fafc; }
QLabel#subtitle { font-size: 12px; color: #94a3b8; }
QLabel#hint { font-size: 12px; color: #94a3b8; }
QLabel#overviewTitle { font-size: 12px; color: #94a3b8; }
QLabel#overviewValue { font-size: 18px; font-weight: 700; color: #f1f5f9; }
QGroupBox {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 8px;
    margin-top: 16px;
    padding: 14px 12px 10px 12px;
    font-weight: 600;
    color: #cbd5e1;
}
QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }
QLineEdit, QPlainTextEdit, QSpinBox, QDoubleSpinBox, QComboBox, QDateEdit {
    background: #0f172a;
    border: 1px solid #334155;
    border-radius: 6px;
    padding: 6px 8px;
    font-weight: normal;
    color: #e5e7eb;
    selection-background-color: #1d4ed8;
    selection-color: #ffffff;
}
QLineEdit:focus, QPlainTextEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus,
QComboBox:focus, QDateEdit:focus { border: 1px solid #3b82f6; }
QLineEdit:disabled, QPlainTextEdit:disabled, QSpinBox:disabled,
QDoubleSpinBox:disabled, QComboBox:disabled, QDateEdit:disabled {
    background: #172033; color: #64748b; border-color: #243244;
}
QCheckBox { font-weight: normal; color: #cbd5e1; }
QCheckBox::indicator {
    width: 14px; height: 14px; border: 1px solid #475569; border-radius: 3px;
    background: #0f172a;
}
QCheckBox::indicator:checked { background: #2563eb; border-color: #2563eb; }
QComboBox QAbstractItemView {
    background: #1e293b; color: #e5e7eb; border: 1px solid #334155;
    selection-background-color: #1d4ed8; selection-color: #ffffff; outline: none;
}
QComboBox QAbstractItemView::item { color: #e5e7eb; padding: 4px 8px; }
QComboBox QAbstractItemView::item:selected { color: #ffffff; background: #1d4ed8; }
/* 微调/下拉/日历按钮：加深底色并绘制亮色箭头，避免与输入框背景混在一起 */
QSpinBox, QDoubleSpinBox { padding-right: 24px; }
QSpinBox::up-button, QDoubleSpinBox::up-button {
    subcontrol-origin: border;
    subcontrol-position: top right;
    width: 20px;
    background: #334155;
    border-left: 1px solid #475569;
    border-top-right-radius: 5px;
}
QSpinBox::down-button, QDoubleSpinBox::down-button {
    subcontrol-origin: border;
    subcontrol-position: bottom right;
    width: 20px;
    background: #334155;
    border-left: 1px solid #475569;
    border-top: 1px solid #475569;
    border-bottom-right-radius: 5px;
}
QSpinBox::up-button:hover, QDoubleSpinBox::up-button:hover,
QSpinBox::down-button:hover, QDoubleSpinBox::down-button:hover { background: #475569; }
QSpinBox::up-button:pressed, QDoubleSpinBox::up-button:pressed,
QSpinBox::down-button:pressed, QDoubleSpinBox::down-button:pressed { background: #64748b; }
QSpinBox::up-arrow, QDoubleSpinBox::up-arrow {
    image: url("@ARROW_UP@"); width: 10px; height: 10px;
}
QSpinBox::down-arrow, QDoubleSpinBox::down-arrow {
    image: url("@ARROW_DOWN@"); width: 10px; height: 10px;
}
/* 下拉箭头由 ::drop-down 占据右侧 22px，右侧再留 padding 会被二次扣减导致文字被裁剪 */
QComboBox { padding-right: 0; }
QComboBox::drop-down {
    subcontrol-origin: padding;
    subcontrol-position: top right;
    width: 22px;
    background: #334155;
    border-left: 1px solid #475569;
    border-top-right-radius: 5px;
    border-bottom-right-radius: 5px;
}
QComboBox::drop-down:hover { background: #475569; }
QComboBox::down-arrow { image: url("@ARROW_DOWN@"); width: 10px; height: 10px; }
QDateEdit { padding-right: 0; }
QDateEdit::drop-down {
    subcontrol-origin: padding;
    subcontrol-position: top right;
    width: 22px;
    background: #334155;
    border-left: 1px solid #475569;
    border-top-right-radius: 5px;
    border-bottom-right-radius: 5px;
}
QDateEdit::drop-down:hover { background: #475569; }
QDateEdit::down-arrow { image: url("@ARROW_DOWN@"); width: 10px; height: 10px; }
QPlainTextEdit#log {
    background: #0b1220; color: #cbd5e1; border: 1px solid #334155;
    font-family: Consolas, 'Courier New', monospace; font-size: 9pt;
}
QScrollArea#upperScroll, QScrollArea#upperScroll > QWidget { background: transparent; border: none; }
QSplitter::handle { background: transparent; }
QSplitter::handle:vertical { height: 8px; }
QSplitter::handle:hover { background: #1e293b; border-radius: 4px; }
QScrollBar:vertical { background: #0f172a; width: 12px; margin: 0; }
QScrollBar::handle:vertical { background: #475569; border-radius: 6px; min-height: 24px; }
QScrollBar:horizontal { background: #0f172a; height: 12px; margin: 0; }
QScrollBar::handle:horizontal { background: #475569; border-radius: 6px; min-width: 24px; }
QScrollBar::add-line, QScrollBar::sub-line { height: 0; width: 0; border: none; }
QScrollBar::add-page, QScrollBar::sub-page { background: transparent; }
QTableWidget {
    background: #111827;
    color: #e5e7eb;
    border: 1px solid #334155;
    border-radius: 6px;
    gridline-color: #334155;
    alternate-background-color: #1b2534;
    selection-background-color: #1d4ed8;
    selection-color: #ffffff;
}
QTableWidget::item { padding: 2px 4px; }
QTableWidget::item:selected { background: #1d4ed8; color: #ffffff; }
QTableCornerButton::section { background: #0f172a; border: none; }
QHeaderView::section {
    background: #1f2937; color: #d1d5db; border: none;
    border-right: 1px solid #334155;
    border-bottom: 1px solid #334155;
    padding: 6px 8px; font-weight: 600;
}
QPushButton {
    background: #1e293b; border: 1px solid #334155; border-radius: 6px;
    padding: 6px 14px; font-weight: normal; color: #e5e7eb;
}
QPushButton:hover { background: #273449; }
QPushButton:pressed { background: #334155; }
QPushButton:disabled { color: #64748b; background: #172033; border-color: #243244; }
QPushButton#primary {
    background: #2563eb; border: 1px solid #2563eb; color: #ffffff; font-weight: 600;
}
QPushButton#primary:hover { background: #1d4ed8; }
QPushButton#primary:disabled { background: #1e3a8a; border-color: #1e3a8a; color: #93c5fd; }
QStatusBar { background: #111827; color: #94a3b8; }
QStatusBar QLabel { color: #94a3b8; }
QToolTip { background: #1e293b; color: #e5e7eb; border: 1px solid #334155; }
QMessageBox { background: #1e293b; }
QMessageBox QLabel { color: #e5e7eb; }
QCalendarWidget QWidget { background: #1e293b; color: #e5e7eb; }
QCalendarWidget QToolButton { color: #e5e7eb; background: transparent; border: none; }
QCalendarWidget QAbstractItemView {
    background: #0f172a; color: #e5e7eb;
    selection-background-color: #1d4ed8; selection-color: #ffffff;
}
""".replace('@ARROW_UP@', ARROW_UP_URI).replace('@ARROW_DOWN@', ARROW_DOWN_URI)

# 预设条件：名称 -> (买入条件, 卖出条件)，每项为 (启用, 左均线索引, 比较符, 右均线索引)
PRESETS = {
    '自定义': None,
    '多头排列：MA1>MA2>MA3': (
        [(True, 0, '>', 1), (True, 1, '>', 2), (False, 0, '>', 2)],
        [(True, 0, '<', 1), (True, 1, '<', 2), (False, 0, '<', 2)],
    ),
    '空头排列：MA1<MA2<MA3': (
        [(True, 0, '<', 1), (True, 1, '<', 2), (False, 0, '<', 2)],
        [(True, 0, '>', 1), (True, 1, '>', 2), (False, 0, '>', 2)],
    ),
    'A>B<C：MA1>MA2 且 MA2<MA3': (
        [(True, 0, '>', 1), (True, 1, '<', 2), (False, 0, '>', 2)],
        [(True, 0, '<', 1), (True, 1, '>', 2), (False, 0, '<', 2)],
    ),
    'A<B>C：MA1<MA2 且 MA2>MA3': (
        [(True, 0, '<', 1), (True, 1, '>', 2), (False, 0, '<', 2)],
        [(True, 0, '>', 1), (True, 1, '<', 2), (False, 0, '>', 2)],
    ),
    '短线金叉：MA1 上穿 MA2': (
        [(True, 0, '>', 1), (False, 1, '>', 2), (False, 0, '>', 2)],
        [(True, 0, '<', 1), (False, 1, '<', 2), (False, 0, '<', 2)],
    ),
}

MA_NAMES = ['MA1', 'MA2', 'MA3']
# 每次卖出比例下拉项：显示文本 -> 比例值
SELL_FRACTIONS = [('1/4（四分之一）', 0.25), ('1/3（三分之一）', 1.0 / 3),
                  ('1/2（二分之一）', 0.5), ('全部', 1.0)]
# 分批买入触发方式：显示文本 -> 参数值（fall=较上次买价下跌，rise=较上次买价上涨）
BUY_MODES = [('每下跌', 'fall'), ('每上涨', 'rise')]
# 分批卖出触发方式：显示文本 -> 参数值（rise=较上次卖价上涨，high=较持仓高点回落）
SELL_MODES = [('每上涨', 'rise'), ('从高点回落', 'high')]
# 成交时机：step=条件成立只更新基准、等涨跌幅达标成交；immediate=条件成立立即成交（旧方式）
SIGNAL_MODES = [('等涨跌幅达标成交', 'step'), ('条件成立立即成交', 'immediate')]
# 明细表按实际成交逐笔展示（每次买入、每次卖出各一行）
TABLE_COLUMNS = ['序号', '成交时间', '方向', '成交价', '数量', '成交金额',
                 '手续费', '盈亏', '盈亏%', '成交后持仓']


class BacktestWorker(QThread):
    """后台执行「取数 + 回测」，避免界面卡死。"""

    succeeded = Signal(dict)
    failed = Signal(str)
    progress = Signal(str)

    def __init__(self, params):
        super().__init__()
        self.params = params

    def run(self):
        try:
            p = self.params
            market_label = {'auto': '自动识别', 'A': 'A股', 'HK': '港股'}.get(p['market'], p['market'])
            self.progress.emit('正在获取行情数据：%s（%s）...' % (p['symbol'], market_label))
            df, code, market = fetch_daily(
                p['symbol'], p['start'], p['end'],
                market=p['market'], adjust=p['adjust'], use_cache=p['use_cache'],
            )
            self.progress.emit('获取到 %d 根 K 线（%s ~ %s），开始回测...' % (
                len(df),
                df['datetime'].iloc[0].strftime('%Y-%m-%d'),
                df['datetime'].iloc[-1].strftime('%Y-%m-%d'),
            ))
            result = run_backtest(
                df,
                ma_periods=p['ma_periods'],
                ma_type=p['ma_type'],
                buy_conditions=p['buy_conditions'],
                sell_conditions=p['sell_conditions'],
                cash_per_trade=p['cash_per_trade'],
                initial_cash_multiple=p['initial_cash_multiple'],
                lot_size=p['lot_size'],
                commission=p['commission'],
                symbol=code,
                market=market,
                step_buy_pct=p['step_buy_pct'],
                step_sell_pct=p['step_sell_pct'],
                sell_fraction=p['sell_fraction'],
                sell_mode=p['sell_mode'],
                buy_mode=p['buy_mode'],
                signal_mode=p['signal_mode'],
            )
            self.succeeded.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))


class ConditionRow(QWidget):
    """一行「启用 + 均线A + 比较符 + 均线B」条件。"""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self.check = QCheckBox('启用')
        self.left_box = QComboBox()
        self.left_box.addItems(MA_NAMES)
        self.left_box.setFixedWidth(84)
        self.op_box = QComboBox()
        self.op_box.addItems(['>', '<'])
        self.op_box.setFixedWidth(64)
        self.right_box = QComboBox()
        self.right_box.addItems(MA_NAMES)
        self.right_box.setFixedWidth(84)

        layout.addWidget(self.check)
        layout.addWidget(self.left_box)
        layout.addWidget(self.op_box)
        layout.addWidget(self.right_box)
        layout.addStretch(1)

    def condition(self):
        if not self.check.isChecked():
            return None
        return (self.left_box.currentIndex(), self.op_box.currentText(),
                self.right_box.currentIndex())

    def set_condition(self, active, left, op, right):
        self.check.setChecked(bool(active))
        self.left_box.setCurrentIndex(int(left))
        self.op_box.setCurrentText(op)
        self.right_box.setCurrentIndex(int(right))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('均线回归回测工具')
        self.resize(1120, 900)

        self.worker = None
        self.last_result = None

        self._build_ui()
        self._restore_settings()
        self.statusBar().showMessage('就绪 · 界面绑定 %s' % QT_BINDING)

    # ---------------- 界面搭建 ----------------
    def _build_ui(self):
        central = QWidget()
        central.setObjectName('centralWidget')
        self.setCentralWidget(central)

        root = QVBoxLayout(central)
        root.setContentsMargins(20, 18, 20, 16)
        root.setSpacing(10)

        title = QLabel('均线回归回测工具')
        title.setObjectName('title')
        subtitle = QLabel('A股 / 港股 · 三条均线自由组合条件 · backtrader + akshare')
        subtitle.setObjectName('subtitle')
        root.addWidget(title)
        root.addWidget(subtitle)

        upper = QWidget()
        upper_layout = QVBoxLayout(upper)
        upper_layout.setContentsMargins(0, 0, 0, 0)
        upper_layout.setSpacing(10)
        upper_layout.addWidget(self._build_target_box())
        upper_layout.addWidget(self._build_ma_box())
        upper_layout.addWidget(self._build_signal_box())
        upper_layout.addWidget(self._build_batch_box())
        upper_layout.addLayout(self._build_action_row())

        upper_scroll = QScrollArea()
        upper_scroll.setObjectName('upperScroll')
        upper_scroll.setWidgetResizable(True)
        upper_scroll.setWidget(upper)
        upper_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        upper_scroll.setMinimumHeight(180)
        upper_scroll.viewport().setAutoFillBackground(False)
        upper.setAutoFillBackground(False)

        self.splitter = QSplitter(Qt.Vertical)
        self.splitter.setChildrenCollapsible(False)
        self.splitter.setHandleWidth(8)
        self.splitter.addWidget(upper_scroll)
        self.splitter.addWidget(self._build_result_box())
        self.splitter.addWidget(self._build_log_box())
        self.splitter.setStretchFactor(0, 0)
        self.splitter.setStretchFactor(1, 1)
        self.splitter.setStretchFactor(2, 0)
        self.splitter.setSizes([400, 300, 150])
        root.addWidget(self.splitter, 1)

    def _build_target_box(self):
        box = QGroupBox('标的与回测区间')
        layout = QVBoxLayout(box)
        layout.setSpacing(8)

        row1 = QHBoxLayout()
        row1.setSpacing(8)
        self.edit_code = QLineEdit('600519')
        self.edit_code.setPlaceholderText('A 股 6 位（如 600519），港股 5 位（如 00700）')
        self.edit_code.setFixedWidth(150)
        self.combo_market = QComboBox()
        self.combo_market.addItems(['自动识别', 'A股', '港股'])
        self.combo_market.setFixedWidth(110)
        self.combo_adjust = QComboBox()
        self.combo_adjust.addItems(['前复权', '后复权', '不复权'])
        self.combo_adjust.setFixedWidth(100)

        row1.addWidget(QLabel('股票代码'))
        row1.addWidget(self.edit_code)
        row1.addSpacing(8)
        row1.addWidget(QLabel('市场'))
        row1.addWidget(self.combo_market)
        row1.addSpacing(8)
        row1.addWidget(QLabel('复权'))
        row1.addWidget(self.combo_adjust)
        row1.addStretch(1)
        layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.setSpacing(8)
        self.date_start = QDateEdit(QDate(2018, 1, 1))
        self.date_start.setCalendarPopup(True)
        self.date_start.setDisplayFormat('yyyy-MM-dd')
        self.date_start.setFixedWidth(130)
        self.date_end = QDateEdit(QDate.currentDate())
        self.date_end.setCalendarPopup(True)
        self.date_end.setDisplayFormat('yyyy-MM-dd')
        self.date_end.setFixedWidth(130)
        self.check_cache = QCheckBox('使用本地行情缓存')
        self.check_cache.setChecked(True)

        row2.addWidget(QLabel('开始日期'))
        row2.addWidget(self.date_start)
        row2.addSpacing(8)
        row2.addWidget(QLabel('结束日期'))
        row2.addWidget(self.date_end)
        row2.addSpacing(16)
        row2.addWidget(self.check_cache)
        row2.addStretch(1)
        layout.addLayout(row2)
        return box

    def _build_ma_box(self):
        box = QGroupBox('资金与均线参数')
        layout = QVBoxLayout(box)
        layout.setSpacing(8)

        row1 = QHBoxLayout()
        row1.setSpacing(8)
        self.spin_cash = QDoubleSpinBox()
        self.spin_cash.setRange(100.0, 1000000000.0)
        self.spin_cash.setDecimals(2)
        self.spin_cash.setSingleStep(10000.0)
        self.spin_cash.setValue(100000.0)
        self.spin_cash.setGroupSeparatorShown(True)
        self.spin_cash.setFixedWidth(150)
        self.spin_cash_multiple = QDoubleSpinBox()
        self.spin_cash_multiple.setRange(0.1, 1000.0)
        self.spin_cash_multiple.setDecimals(1)
        self.spin_cash_multiple.setSingleStep(1.0)
        self.spin_cash_multiple.setValue(20.0)
        self.spin_cash_multiple.setSuffix(' 倍')
        self.spin_cash_multiple.setFixedWidth(100)
        self.spin_cash_multiple.setToolTip(
            '初始资金 = 每次买入金额 × 该倍数，最低 %s 元' % format(MIN_INITIAL_CASH, ',.0f'))
        self.spin_lot = QSpinBox()
        self.spin_lot.setRange(1, 100000)
        self.spin_lot.setValue(100)
        self.spin_lot.setFixedWidth(100)
        self.spin_commission = QDoubleSpinBox()
        self.spin_commission.setRange(0.0, 0.05)
        self.spin_commission.setDecimals(5)
        self.spin_commission.setSingleStep(0.0001)
        self.spin_commission.setValue(0.0005)
        self.spin_commission.setFixedWidth(110)

        row1.addWidget(QLabel('每次买入金额'))
        row1.addWidget(self.spin_cash)
        row1.addSpacing(12)
        row1.addWidget(QLabel('初始资金倍数'))
        row1.addWidget(self.spin_cash_multiple)
        row1.addSpacing(12)
        row1.addWidget(QLabel('每手股数'))
        row1.addWidget(self.spin_lot)
        row1.addSpacing(12)
        row1.addWidget(QLabel('佣金率（双边）'))
        row1.addWidget(self.spin_commission)
        row1.addStretch(1)
        layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.setSpacing(8)
        self.spin_ma1 = QSpinBox()
        self.spin_ma2 = QSpinBox()
        self.spin_ma3 = QSpinBox()
        for spin, value in ((self.spin_ma1, 5), (self.spin_ma2, 20), (self.spin_ma3, 60)):
            spin.setRange(1, 500)
            spin.setValue(value)
            spin.setFixedWidth(84)
        self.combo_ma_type = QComboBox()
        self.combo_ma_type.addItems(['SMA（简单均线）', 'EMA（指数均线）'])
        self.combo_ma_type.setFixedWidth(170)

        row2.addWidget(QLabel('MA1 周期'))
        row2.addWidget(self.spin_ma1)
        row2.addSpacing(8)
        row2.addWidget(QLabel('MA2 周期'))
        row2.addWidget(self.spin_ma2)
        row2.addSpacing(8)
        row2.addWidget(QLabel('MA3 周期'))
        row2.addWidget(self.spin_ma3)
        row2.addSpacing(12)
        row2.addWidget(QLabel('均线类型'))
        row2.addWidget(self.combo_ma_type)
        row2.addStretch(1)
        layout.addLayout(row2)

        hint = QLabel('均线基于收盘价计算；信号在当日收盘产生，委托按次日开盘价成交。'
                      '每次买入金额需覆盖「1 手 = 每手股数 × 股价」，否则该信号会被跳过。'
                      '初始资金 = 每次买入金额 × 初始资金倍数，最低 %s 元。'
                      % format(MIN_INITIAL_CASH, ',.0f'))
        hint.setObjectName('hint')
        hint.setWordWrap(True)
        layout.addWidget(hint)
        return box

    def _build_signal_box(self):
        box = QGroupBox('买卖条件组合')
        layout = QVBoxLayout(box)
        layout.setSpacing(8)

        row0 = QHBoxLayout()
        row0.setSpacing(8)
        self.combo_preset = QComboBox()
        self.combo_preset.addItems(list(PRESETS.keys()))
        self.combo_preset.setFixedWidth(300)
        self.combo_preset.currentTextChanged.connect(self._apply_preset)
        row0.addWidget(QLabel('条件预设'))
        row0.addWidget(self.combo_preset)
        hint = QLabel('多条条件之间为「且」关系，可自由组合')
        hint.setObjectName('hint')
        row0.addWidget(hint)
        row0.addStretch(1)
        layout.addLayout(row0)

        self.buy_rows = []
        buy_title = QLabel('买入条件（全部成立时买入，空仓或持仓均适用）')
        buy_title.setStyleSheet('color: #f87171; font-weight: 600;')
        layout.addWidget(buy_title)
        for _ in range(3):
            row = ConditionRow()
            self.buy_rows.append(row)
            layout.addWidget(row)

        self.sell_rows = []
        sell_title = QLabel('卖出条件（持仓且全部成立时按下方「分批买卖设置」卖出）')
        sell_title.setStyleSheet('color: #4ade80; font-weight: 600;')
        layout.addWidget(sell_title)
        for _ in range(3):
            row = ConditionRow()
            self.sell_rows.append(row)
            layout.addWidget(row)

        # 默认使用多头排列作为示例条件（setCurrentText 会触发 _apply_preset）
        self.combo_preset.setCurrentText('多头排列：MA1>MA2>MA3')
        return box

    def _build_batch_box(self):
        box = QGroupBox('分批买卖设置')
        layout = QVBoxLayout(box)
        layout.setSpacing(8)

        row0 = QHBoxLayout()
        row0.setSpacing(8)
        self.combo_signal_mode = QComboBox()
        self.combo_signal_mode.addItems([text for text, _ in SIGNAL_MODES])
        self.combo_signal_mode.setFixedWidth(180)
        row0.addWidget(QLabel('成交时机：条件成立后'))
        row0.addWidget(self.combo_signal_mode)
        row0.addWidget(QLabel('（立即成交为旧方式；等涨跌幅则先只更新基准）'))
        row0.addStretch(1)
        layout.addLayout(row0)

        row1 = QHBoxLayout()
        row1.setSpacing(8)
        self.combo_buy_mode = QComboBox()
        self.combo_buy_mode.addItems([text for text, _ in BUY_MODES])
        self.combo_buy_mode.setFixedWidth(130)
        self.spin_buy_step = QDoubleSpinBox()
        self.spin_buy_step.setRange(0.0, 50.0)
        self.spin_buy_step.setDecimals(2)
        self.spin_buy_step.setSingleStep(0.5)
        self.spin_buy_step.setValue(0.0)
        self.spin_buy_step.setSuffix(' %')
        self.spin_buy_step.setFixedWidth(110)

        row1.addWidget(QLabel('买入：条件成立后'))
        row1.addWidget(self.combo_buy_mode)
        row1.addWidget(self.spin_buy_step)
        row1.addWidget(QLabel('买入一次，每次金额固定为「每次买入金额」'))
        row1.addStretch(1)
        layout.addLayout(row1)
        self.spin_buy_step.valueChanged.connect(self._sync_buy_mode)

        row2 = QHBoxLayout()
        row2.setSpacing(8)
        self.combo_sell_mode = QComboBox()
        self.combo_sell_mode.addItems([text for text, _ in SELL_MODES])
        self.combo_sell_mode.setFixedWidth(130)
        self.spin_sell_step = QDoubleSpinBox()
        self.spin_sell_step.setRange(0.0, 50.0)
        self.spin_sell_step.setDecimals(2)
        self.spin_sell_step.setSingleStep(0.5)
        self.spin_sell_step.setValue(0.0)
        self.spin_sell_step.setSuffix(' %')
        self.spin_sell_step.setFixedWidth(110)
        self.combo_sell_fraction = QComboBox()
        self.combo_sell_fraction.addItems([text for text, _ in SELL_FRACTIONS])
        self.combo_sell_fraction.setCurrentIndex(len(SELL_FRACTIONS) - 1)
        self.combo_sell_fraction.setFixedWidth(150)
        self.spin_sell_step.valueChanged.connect(self._sync_sell_fraction)

        row2.addWidget(QLabel('卖出：条件成立后'))
        row2.addWidget(self.combo_sell_mode)
        row2.addWidget(self.spin_sell_step)
        row2.addWidget(QLabel('时减仓一次，每次卖出持仓的'))
        row2.addWidget(self.combo_sell_fraction)
        row2.addStretch(1)
        layout.addLayout(row2)

        hint = QLabel('成交时机可选「等涨跌幅达标成交」或「条件成立立即成交」：前者在条件成立时'
                      '只更新基准价格，价格相对基准变动达到设定幅度才成交；后者条件成立即成交，'
                      '之后按间隔继续买卖。买入基准为条件成立时收盘价（成交后为成交价），'
                      '卖出「每上涨」以收盘价、「从高点回落」以最高价为基准；'
                      '填 0 表示条件成立即成交；条件失效后清空基准，重新成立时按新价格重建。')
        hint.setObjectName('hint')
        hint.setWordWrap(True)
        layout.addWidget(hint)

        self._sync_buy_mode()
        self._sync_sell_fraction()
        return box

    def _sync_buy_mode(self, value=None):
        """买入间隔为 0 时不分批，触发方式下拉框随之禁用。"""
        self.combo_buy_mode.setEnabled(self.spin_buy_step.value() > 0)

    def _sync_sell_fraction(self, value=None):
        """卖出间隔为 0 时不做分批，直接一次性清仓，触发方式与比例下拉框随之禁用。"""
        enabled = self.spin_sell_step.value() > 0
        self.combo_sell_fraction.setEnabled(enabled)
        self.combo_sell_mode.setEnabled(enabled)

    def _build_action_row(self):
        row = QHBoxLayout()
        row.setSpacing(8)
        self.btn_run = QPushButton('开始回测')
        self.btn_run.setObjectName('primary')
        self.btn_run.setMinimumWidth(120)
        self.btn_run.clicked.connect(self._start_backtest)
        self.btn_export = QPushButton('导出明细 CSV')
        self.btn_export.setEnabled(False)
        self.btn_export.clicked.connect(self._export_trades)
        self.btn_clear = QPushButton('清空日志')
        self.btn_clear.clicked.connect(lambda: self.log_view.clear())

        row.addWidget(self.btn_run)
        row.addWidget(self.btn_export)
        row.addWidget(self.btn_clear)
        row.addStretch(1)
        return row

    def _build_result_box(self):
        box = QGroupBox('回测结果')
        layout = QVBoxLayout(box)
        layout.setSpacing(8)

        self.label_summary = QLabel('尚未回测')
        self.label_summary.setObjectName('hint')
        layout.addWidget(self.label_summary)

        overview = QHBoxLayout()
        overview.setSpacing(24)
        self.overview_labels = {}
        for key, text in (('total_pnl', '总盈亏（元）'), ('realized_pnl', '已实现盈亏'),
                          ('unrealized_pnl', '浮动盈亏'), ('buy_count', '买入次数'),
                          ('sell_count', '卖出次数'), ('win_rate', '胜率')):
            item = QVBoxLayout()
            item.setSpacing(2)
            title = QLabel(text)
            title.setObjectName('overviewTitle')
            value = QLabel('-')
            value.setObjectName('overviewValue')
            item.addWidget(title)
            item.addWidget(value)
            overview.addLayout(item)
            self.overview_labels[key] = value
        overview.addStretch(1)
        layout.addLayout(overview)

        self.label_anchor = QLabel('基准价格：尚未回测')
        self.label_anchor.setObjectName('hint')
        self.label_anchor.setWordWrap(True)
        layout.addWidget(self.label_anchor)

        self.table = QTableWidget(0, len(TABLE_COLUMNS))
        self.table.setHorizontalHeaderLabels(TABLE_COLUMNS)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Stretch)
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        layout.addWidget(self.table, 1)
        return box

    def _build_log_box(self):
        box = QGroupBox('运行日志')
        layout = QVBoxLayout(box)
        layout.setSpacing(6)
        self.log_view = QPlainTextEdit()
        self.log_view.setObjectName('log')
        self.log_view.setReadOnly(True)
        self.log_view.setMaximumBlockCount(2000)
        layout.addWidget(self.log_view)
        return box

    # ---------------- 配置持久化 ----------------
    def _settings(self):
        """创建指向脚本同目录 settings.ini 的 QSettings（ini 格式便于查看/手工删除）。"""
        return QSettings(str(SETTINGS_PATH), QSettings.IniFormat)

    def _save_settings(self):
        """把界面上的全部参数写入本地 ini，下次启动时自动还原。"""
        settings = self._settings()
        settings.setValue('target/code', self.edit_code.text())
        settings.setValue('target/market_index', self.combo_market.currentIndex())
        settings.setValue('target/adjust_index', self.combo_adjust.currentIndex())
        settings.setValue('target/start', self.date_start.date().toString('yyyy-MM-dd'))
        settings.setValue('target/end', self.date_end.date().toString('yyyy-MM-dd'))
        settings.setValue('target/use_cache', self.check_cache.isChecked())

        settings.setValue('params/cash', self.spin_cash.value())
        settings.setValue('params/cash_multiple', self.spin_cash_multiple.value())
        settings.setValue('params/lot_size', self.spin_lot.value())
        settings.setValue('params/commission', self.spin_commission.value())
        settings.setValue('params/ma1', self.spin_ma1.value())
        settings.setValue('params/ma2', self.spin_ma2.value())
        settings.setValue('params/ma3', self.spin_ma3.value())
        settings.setValue('params/ma_type_index', self.combo_ma_type.currentIndex())

        settings.setValue('batch/buy_step', self.spin_buy_step.value())
        settings.setValue('batch/sell_step', self.spin_sell_step.value())
        settings.setValue('batch/signal_mode_index', self.combo_signal_mode.currentIndex())
        settings.setValue('batch/buy_mode_index', self.combo_buy_mode.currentIndex())
        settings.setValue('batch/sell_fraction_index', self.combo_sell_fraction.currentIndex())
        settings.setValue('batch/sell_mode_index', self.combo_sell_mode.currentIndex())

        settings.setValue('signal/preset', self.combo_preset.currentText())
        for prefix, rows in (('signal/buy', self.buy_rows), ('signal/sell', self.sell_rows)):
            for index, row in enumerate(rows):
                # 单行条件序列化为「启用|左均线|比较符|右均线」，避免列表被 ini 按逗号拆分
                settings.setValue('%s/%d' % (prefix, index), '|'.join((
                    '1' if row.check.isChecked() else '0',
                    str(row.left_box.currentIndex()),
                    row.op_box.currentText(),
                    str(row.right_box.currentIndex()),
                )))
        settings.sync()

    def _restore_settings(self):
        """读取上次保存的配置；文件缺失或单项异常时保留界面默认值。"""
        if not SETTINGS_PATH.exists():
            return
        settings = self._settings()

        code = settings.value('target/code')
        if code:
            self.edit_code.setText(str(code))
        self._set_combo_index(self.combo_market, settings.value('target/market_index'))
        self._set_combo_index(self.combo_adjust, settings.value('target/adjust_index'))
        for key, widget in (('target/start', self.date_start), ('target/end', self.date_end)):
            text = settings.value(key)
            if text:
                date = QDate.fromString(str(text), 'yyyy-MM-dd')
                if date.isValid():
                    widget.setDate(date)
        self.check_cache.setChecked(_to_bool(settings.value('target/use_cache'),
                                            self.check_cache.isChecked()))

        self.spin_cash.setValue(_to_float(settings.value('params/cash'), self.spin_cash.value()))
        self.spin_cash_multiple.setValue(_to_float(settings.value('params/cash_multiple'),
                                                   self.spin_cash_multiple.value()))
        self.spin_lot.setValue(_to_int(settings.value('params/lot_size'), self.spin_lot.value()))
        self.spin_commission.setValue(_to_float(settings.value('params/commission'),
                                                self.spin_commission.value()))
        self.spin_ma1.setValue(_to_int(settings.value('params/ma1'), self.spin_ma1.value()))
        self.spin_ma2.setValue(_to_int(settings.value('params/ma2'), self.spin_ma2.value()))
        self.spin_ma3.setValue(_to_int(settings.value('params/ma3'), self.spin_ma3.value()))
        self._set_combo_index(self.combo_ma_type, settings.value('params/ma_type_index'))

        self.spin_buy_step.setValue(_to_float(settings.value('batch/buy_step'),
                                              self.spin_buy_step.value()))
        self.spin_sell_step.setValue(_to_float(settings.value('batch/sell_step'),
                                               self.spin_sell_step.value()))
        self._set_combo_index(self.combo_signal_mode,
                              settings.value('batch/signal_mode_index'))
        self._set_combo_index(self.combo_buy_mode, settings.value('batch/buy_mode_index'))
        self._set_combo_index(self.combo_sell_fraction,
                              settings.value('batch/sell_fraction_index'))
        self._set_combo_index(self.combo_sell_mode, settings.value('batch/sell_mode_index'))
        self._sync_buy_mode()
        self._sync_sell_fraction()

        # 先还原预设（切换预设会覆盖条件行），再逐行还原条件
        preset = settings.value('signal/preset')
        if preset is not None and self.combo_preset.findText(str(preset)) >= 0:
            self.combo_preset.setCurrentText(str(preset))

        for prefix, rows in (('signal/buy', self.buy_rows), ('signal/sell', self.sell_rows)):
            for index, row in enumerate(rows):
                raw = settings.value('%s/%d' % (prefix, index))
                if not raw:
                    continue
                parts = str(raw).split('|')
                if len(parts) != 4:
                    continue
                row.set_condition(
                    _to_bool(parts[0], True),
                    _to_int(parts[1], row.left_box.currentIndex()),
                    parts[2] if parts[2] in ('>', '<') else row.op_box.currentText(),
                    _to_int(parts[3], row.right_box.currentIndex()),
                )

    @staticmethod
    def _set_combo_index(combo, value):
        """仅在取值合法时设置下拉框项，避免越界或字符串报错。"""
        index = _to_int(value, combo.currentIndex())
        if 0 <= index < combo.count():
            combo.setCurrentIndex(index)

    # ---------------- 交互逻辑 ----------------
    def _apply_preset(self, name):
        preset = PRESETS.get(name)
        if not preset:
            return
        buy, sell = preset
        for row, values in zip(self.buy_rows + self.sell_rows, list(buy) + list(sell)):
            row.set_condition(*values)

    def _append_log(self, text):
        self.log_view.appendPlainText(text)

    def _sell_fraction(self):
        """取当前选择的每次卖出比例（1/4、1/3、1/2 或 1）。"""
        index = max(self.combo_sell_fraction.currentIndex(), 0)
        return SELL_FRACTIONS[index][1]

    def _sell_mode(self):
        """取当前选择的卖出触发方式（rise=每上涨，high=从高点回落）。"""
        index = max(self.combo_sell_mode.currentIndex(), 0)
        return SELL_MODES[index][1]

    def _buy_mode(self):
        """取当前选择的买入触发方式（fall=每下跌，rise=每上涨）。"""
        index = max(self.combo_buy_mode.currentIndex(), 0)
        return BUY_MODES[index][1]

    def _signal_mode(self):
        """取当前选择的成交时机（step=等涨跌幅，immediate=条件成立立即成交）。"""
        index = max(self.combo_signal_mode.currentIndex(), 0)
        return SIGNAL_MODES[index][1]

    def _collect_params(self):
        symbol = self.edit_code.text().strip()
        if not symbol:
            QMessageBox.warning(self, '参数错误', '请输入股票代码')
            return None

        buy_conditions = [c for c in (row.condition() for row in self.buy_rows) if c]
        sell_conditions = [c for c in (row.condition() for row in self.sell_rows) if c]
        if not buy_conditions:
            QMessageBox.warning(self, '参数错误', '请至少启用一条买入条件')
            return None
        if not sell_conditions:
            QMessageBox.warning(self, '参数错误', '请至少启用一条卖出条件')
            return None

        start = self.date_start.date().toString('yyyy-MM-dd')
        end = self.date_end.date().toString('yyyy-MM-dd')
        if start >= end:
            QMessageBox.warning(self, '参数错误', '开始日期必须早于结束日期')
            return None

        market_map = {'自动识别': 'auto', 'A股': 'A', '港股': 'HK'}
        adjust_map = {'前复权': 'qfq', '后复权': 'hfq', '不复权': ''}
        ma_type = 'EMA' if self.combo_ma_type.currentIndex() == 1 else 'SMA'

        return dict(
            symbol=symbol,
            market=market_map[self.combo_market.currentText()],
            adjust=adjust_map[self.combo_adjust.currentText()],
            use_cache=self.check_cache.isChecked(),
            start=start,
            end=end,
            ma_periods=(self.spin_ma1.value(), self.spin_ma2.value(), self.spin_ma3.value()),
            ma_type=ma_type,
            buy_conditions=tuple(buy_conditions),
            sell_conditions=tuple(sell_conditions),
            cash_per_trade=self.spin_cash.value(),
            initial_cash_multiple=self.spin_cash_multiple.value(),
            lot_size=self.spin_lot.value(),
            commission=self.spin_commission.value(),
            step_buy_pct=self.spin_buy_step.value(),
            step_sell_pct=self.spin_sell_step.value(),
            sell_fraction=self._sell_fraction(),
            sell_mode=self._sell_mode(),
            buy_mode=self._buy_mode(),
            signal_mode=self._signal_mode(),
        )

    def _start_backtest(self):
        if self.worker is not None and self.worker.isRunning():
            return
        params = self._collect_params()
        if params is None:
            return

        self.last_result = None
        self.table.setRowCount(0)
        self.btn_export.setEnabled(False)
        for label in self.overview_labels.values():
            label.setText('-')
            label.setStyleSheet('')
        self.label_summary.setText('回测运行中...')
        self.label_anchor.setText('基准价格：回测运行中...')
        self.log_view.clear()
        self.btn_run.setEnabled(False)
        self.statusBar().showMessage('回测运行中...')

        initial_cash = max(params['cash_per_trade'] * params['initial_cash_multiple'],
                           MIN_INITIAL_CASH)
        self._append_log('回测参数：%s，区间 %s ~ %s，均线 %s(%s)，每次买入 %.2f 元，'
                         '初始资金 %.2f 元（%s 倍）' % (
                             params['symbol'], params['start'], params['end'],
                             '/'.join(str(p) for p in params['ma_periods']), params['ma_type'],
                             params['cash_per_trade'], initial_cash,
                             params['initial_cash_multiple']))
        buy_batch, sell_batch = describe_batch(params['step_buy_pct'], params['step_sell_pct'],
                                               params['sell_fraction'], params['sell_mode'],
                                               params['buy_mode'], params['signal_mode'])
        self._append_log('分批设置：成交时机 %s；买入 %s；卖出 %s' % (
            '条件成立立即成交' if params['signal_mode'] == 'immediate' else '等涨跌幅达标成交',
            buy_batch, sell_batch))

        self.worker = BacktestWorker(params)
        self.worker.progress.connect(self._append_log)
        self.worker.succeeded.connect(self._on_backtest_ok)
        self.worker.failed.connect(self._on_backtest_fail)
        self.worker.finished.connect(self._on_backtest_finished)
        self.worker.start()

    def _on_backtest_ok(self, result):
        self.last_result = result
        positive = result['total_pnl'] > 0
        color = '#f87171' if positive else ('#4ade80' if result['total_pnl'] < 0 else '#e5e7eb')

        self.overview_labels['total_pnl'].setText(format(result['total_pnl'], '+,.2f'))
        self.overview_labels['total_pnl'].setStyleSheet('color: %s;' % color)
        self.overview_labels['realized_pnl'].setText(format(result['realized_pnl'], '+,.2f'))
        self.overview_labels['unrealized_pnl'].setText(format(result['unrealized_pnl'], '+,.2f'))
        self.overview_labels['buy_count'].setText('%d 次' % result['buy_count'])
        self.overview_labels['sell_count'].setText('%d 次' % result['sell_count'])
        self.overview_labels['win_rate'].setText('%.1f%%' % result['win_rate'])

        buy_batch, sell_batch = describe_batch(result['step_buy_pct'], result['step_sell_pct'],
                                               result['sell_fraction'], result['sell_mode'],
                                               result['buy_mode'], result['signal_mode'])
        self.label_summary.setText(
            '%s（%s）%s ~ %s，%d 根 K 线｜买入条件：%s｜卖出条件：%s｜成交时机：%s'
            '｜分批：买入 %s，卖出 %s｜已平仓 %d 笔，手续费合计 %.2f 元'
            % (result['symbol'], result['market_name'],
               result['start'].strftime('%Y-%m-%d'), result['end'].strftime('%Y-%m-%d'),
               result['bars'],
               describe_conditions(result['buy_conditions'], result['ma_periods']),
               describe_conditions(result['sell_conditions'], result['ma_periods']),
               '条件成立立即成交' if result['signal_mode'] == 'immediate' else '等涨跌幅达标成交',
               buy_batch, sell_batch,
               result['closed_count'], result['commission_total']))

        self.label_anchor.setText(self._anchor_text(result))
        self._fill_table(result)
        self.btn_export.setEnabled(bool(result['orders']))

        self._append_log('回测完成：总盈亏 %.2f 元（已实现 %.2f，浮动 %.2f）' % (
            result['total_pnl'], result['realized_pnl'], result['unrealized_pnl']))
        self._append_log('买入 %d 次，卖出 %d 次，已平仓 %d 笔，胜率 %.2f%%' % (
            result['buy_count'], result['sell_count'], result['closed_count'], result['win_rate']))
        if result['open_position']:
            position = result['open_position']
            self._append_log('未平仓：%d 股，均价 %.3f，最新价 %.3f，浮动盈亏 %+.2f 元' % (
                position['size'], position['buy_price'], position['last_price'], position['pnl']))
        if result['failed_buy_count']:
            # 把「条件成立但没买成」的情况列出，避免只看到买入次数少却不知道原因
            self._append_log('买入失败 %d 次（条件成立但未能成交）：' % result['failed_buy_count'])
            for item in result['failed_buys'][:10]:
                self._append_log('  %s @ %.3f 未买入：%s' % (
                    item['time'].strftime('%Y-%m-%d'), item['price'], item['reason']))
            if result['failed_buy_count'] > 10:
                self._append_log('  ...其余 %d 条未列出' % (result['failed_buy_count'] - 10))
        if not result['buy_count']:
            self._append_log('本次回测没有触发任何买入信号，可调整均线周期或条件组合')
        self.statusBar().showMessage('回测完成')

    def _on_backtest_fail(self, message):
        self.label_summary.setText('回测失败：%s' % message)
        self._append_log('回测失败：%s' % message)
        self.statusBar().showMessage('回测失败')
        QMessageBox.critical(self, '回测失败', message)

    def _on_backtest_finished(self):
        self.btn_run.setEnabled(True)

    def _anchor_text(self, result):
        """生成基准价格说明：先讲触发规则，再给出本次回测结束时的基准值。"""
        anchors = result['anchors']
        step_buy = result['step_buy_pct']
        step_sell = result['step_sell_pct']
        fraction = fraction_text(result['sell_fraction'])

        immediate = str(result.get('signal_mode', 'step')) == 'immediate'
        trend = '上涨' if result['buy_mode'] == 'rise' else '下跌'
        if step_buy <= 0:
            buy_rule = '买入不限价格间隔，条件成立即买入'
        elif immediate:
            buy_rule = ('买入条件成立即成交，之后以上一笔买入成交价为基准，再%s %.2f%% 才加仓'
                        % (trend, step_buy))
        else:
            buy_rule = '买入条件成立时以当时收盘价为基准，再%s %.2f%% 才买入' % (trend, step_buy)
        if immediate:
            buy_rule += '；买入条件失效后基准清空，重新成立时立即买入'
        else:
            buy_rule += '；成交后基准更新为该笔成交价；条件失效后基准清空，重新成立时按新价格重建'

        if step_sell <= 0:
            sell_rule = '卖出信号成立时一次性清仓，不使用价格基准'
        elif immediate:
            if result['sell_mode'] == 'high':
                sell_rule = ('卖出以持仓期间最高价为基准，回落 %.2f%% 时卖出持仓的 %s；'
                             '卖出一笔后基准重置为该笔卖出成交价；'
                             '条件失效后基准清空，下次成立时立即卖出'
                             % (step_sell, fraction))
            else:
                sell_rule = ('卖出条件成立即先卖出一次，之后以上一笔卖出成交价为基准，'
                             '再上涨 %.2f%% 时卖出持仓的 %s；'
                             '条件失效后基准清空，下次成立时立即卖出'
                             % (step_sell, fraction))
        elif result['sell_mode'] == 'high':
            sell_rule = ('卖出条件成立时以当时最高价为基准，回落 %.2f%% 时卖出持仓的 %s；'
                         '卖出一笔后基准重置为该笔卖出成交价；'
                         '条件失效后基准清空，重新成立时按新价格重建'
                         % (step_sell, fraction))
        else:
            sell_rule = ('卖出条件成立时以当时收盘价为基准，再上涨 %.2f%% 时卖出持仓的 %s；'
                         '卖出一笔后基准更新为该笔成交价；'
                         '条件失效后基准清空，重新成立时按新价格重建'
                         % (step_sell, fraction))

        parts = ['当前买入基准%s' % self._anchor_value(anchors['buy_anchor'],
                                                      anchors['buy_reset'])]
        if step_sell > 0:
            if result['sell_mode'] == 'high':
                parts.append('当前持仓最高价%s' % self._anchor_value(anchors['peak_price'],
                                                                 anchors['sell_reset']))
            parts.append('当前卖出基准%s' % self._anchor_value(anchors['sell_anchor'],
                                                            anchors['sell_reset']))
        if not result['open_position']:
            parts.append('回测结束时空仓')

        return '基准价格：%s｜%s｜本次回测结束：%s' % (buy_rule, sell_rule, '，'.join(parts))

    @staticmethod
    def _anchor_value(value, reset):
        """把基准数值转成展示文本：已清空、无、或具体价格。"""
        if reset:
            return '已清空（等待条件重新成立）'
        if value is None:
            return '无'
        return ' %.3f 元' % value

    def _fill_table(self, result):
        """按实际成交逐笔展示：每次买入、每次卖出一条记录。"""
        orders = result['orders']
        self.table.setRowCount(len(orders))
        right = Qt.AlignRight | Qt.AlignVCenter
        for row_index, item in enumerate(orders):
            is_buy = item['side'] == 'buy'
            pnl = item['pnl']
            cells = [
                str(row_index + 1),
                item['time'].strftime('%Y-%m-%d'),
                '买入' if is_buy else '卖出',
                '%.3f' % item['price'],
                str(item['size']),
                '%.2f' % item['amount'],
                '%.2f' % item['comm'],
                '-' if pnl is None else '%+.2f' % pnl,
                '-' if pnl is None else '%+.2f%%' % item['pnl_pct'],
                str(item['position_after']),
            ]
            for col_index, text in enumerate(cells):
                cell = QTableWidgetItem(text)
                cell.setTextAlignment(right if col_index >= 3
                                      else (Qt.AlignCenter | Qt.AlignVCenter))
                if col_index == 2:
                    # 买入用红色、卖出用绿色，深色背景下取更亮的色值
                    cell.setForeground(QColor('#f87171' if is_buy else '#4ade80'))
                elif col_index == 7 and pnl is not None:
                    cell.setForeground(QColor('#f87171' if pnl > 0 else '#4ade80'))
                self.table.setItem(row_index, col_index, cell)

    def _export_trades(self):
        result = self.last_result
        if not result:
            return
        default_name = '%s_%s_ma_trades.csv' % (result['symbol'], result['ma_type'])
        path, _ = QFileDialog.getSaveFileName(self, '导出交易明细', default_name, 'CSV 文件 (*.csv)')
        if not path:
            return

        lines = ['序号,方向,成交时间,成交价,数量,成交金额,手续费,盈亏,盈亏率,成交后持仓']
        for index, item in enumerate(result['orders'], start=1):
            pnl_text = '' if item['pnl'] is None else '%.2f' % item['pnl']
            pct_text = '' if item['pnl'] is None else '%.2f%%' % item['pnl_pct']
            lines.append('%d,%s,%s,%.3f,%d,%.2f,%.2f,%s,%s,%d' % (
                index, '买入' if item['side'] == 'buy' else '卖出',
                item['time'].strftime('%Y-%m-%d %H:%M'), item['price'], item['size'],
                item['amount'], item['comm'], pnl_text, pct_text, item['position_after']))

        try:
            Path(path).write_text('\n'.join(lines), encoding='utf-8-sig')
        except Exception as exc:
            QMessageBox.critical(self, '导出失败', str(exc))
            return
        self.statusBar().showMessage('明细已导出：%s' % path)
        self._append_log('明细已导出：%s' % path)

    def closeEvent(self, event):
        # 关闭前先把界面参数写入本地，保证下次启动沿用本次配置
        try:
            self._save_settings()
        except Exception as exc:
            # 配置保存失败不应阻塞退出，仅在日志中提示
            self._append_log('界面配置保存失败：%s' % exc)

        # 回测线程仍在取数时关闭窗口会导致线程被销毁，这里先等待其结束
        if self.worker is not None and self.worker.isRunning():
            if not self.worker.wait(3000):
                self.worker.terminate()
                self.worker.wait(1000)
        super().closeEvent(event)


def apply_dark_theme(app):
    """统一暗色主题：Fusion 风格 + 暗色调色板 + 样式表。

    Fusion 风格配合暗色调色板，能保证下拉箭头、日历弹窗、滚动条箭头等
    由 Qt 绘制的元素也是亮色，不会因系统浅色调色板出现深色图标。
    """
    app.setStyle('Fusion')
    role = getattr(QPalette, 'ColorRole', QPalette)
    palette = QPalette()
    palette.setColor(role.Window, QColor('#0f172a'))
    palette.setColor(role.WindowText, QColor('#e5e7eb'))
    palette.setColor(role.Base, QColor('#111827'))
    palette.setColor(role.AlternateBase, QColor('#1b2534'))
    palette.setColor(role.Text, QColor('#e5e7eb'))
    palette.setColor(role.Button, QColor('#1e293b'))
    palette.setColor(role.ButtonText, QColor('#e5e7eb'))
    palette.setColor(role.ToolTipBase, QColor('#1e293b'))
    palette.setColor(role.ToolTipText, QColor('#e5e7eb'))
    palette.setColor(role.Highlight, QColor('#2563eb'))
    palette.setColor(role.HighlightedText, QColor('#ffffff'))
    palette.setColor(role.PlaceholderText, QColor('#64748b'))
    app.setPalette(palette)
    app.setStyleSheet(STYLE_SHEET)


def main():
    _ensure_default_settings()
    app = QApplication(sys.argv)
    apply_dark_theme(app)
    font = QFont('Microsoft YaHei UI', 9)
    app.setFont(font)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_() if QT_BINDING == 'PyQt5' else app.exec())


if __name__ == '__main__':
    main()
