"""财务指标分析工具的 Qt 图形界面。

把 baobiaofenxi.py 的命令行参数（配置文件、输出目录）做成界面选项，
以子进程方式调用原脚本，并实时显示运行日志。
依赖：PyQt5（未安装时自动回退到 PySide6）。
"""

import configparser
import re
import shutil
import sys
import tempfile
from pathlib import Path

# Qt 绑定兼容导入：优先 PyQt5，其次 PySide6
try:
    from PyQt5.QtCore import QProcess, QProcessEnvironment, Qt, QUrl
    from PyQt5.QtGui import QColor, QDesktopServices, QFont, QTextCharFormat, QTextCursor
    from PyQt5.QtWidgets import (
        QApplication,
        QButtonGroup,
        QCheckBox,
        QComboBox,
        QDoubleSpinBox,
        QFileDialog,
        QGridLayout,
        QGroupBox,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMessageBox,
        QPlainTextEdit,
        QPushButton,
        QRadioButton,
        QScrollArea,
        QSplitter,
        QVBoxLayout,
        QWidget,
    )

    QT_BINDING = 'PyQt5'
except ImportError:
    from PySide6.QtCore import QProcess, QProcessEnvironment, Qt, QUrl
    from PySide6.QtGui import QColor, QDesktopServices, QFont, QTextCharFormat, QTextCursor
    from PySide6.QtWidgets import (
        QApplication,
        QButtonGroup,
        QCheckBox,
        QComboBox,
        QDoubleSpinBox,
        QFileDialog,
        QGridLayout,
        QGroupBox,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMessageBox,
        QPlainTextEdit,
        QPushButton,
        QRadioButton,
        QScrollArea,
        QSplitter,
        QVBoxLayout,
        QWidget,
    )

    QT_BINDING = 'PySide6'

SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPT_PATH = SCRIPT_DIR / 'baobiaofenxi.py'
DEFAULT_CONFIG = SCRIPT_DIR / 'config.ini'
DEFAULT_OUTPUT = SCRIPT_DIR / 'output'
# 界面参数持久化文件，与脚本同目录
SETTINGS_PATH = SCRIPT_DIR / 'setting.ini'

# 可设置阈值的关键指标（名称须与 baobiaofenxi.py 的日志输出前缀一致）
RULE_ITEMS = (
    '资产负债率',
    '三项费用比重',
    '净资产收益率(%)',
    '应收账款周转率(次)',
    '存货周转率(次)',
    '存货周转评分',
    '速动比率',
    '流动比率',
    '现金比率(%)',
)

# 日志着色：命中阈值显示红色，未命中显示绿色
COLOR_HIT = QColor('#dc2626')
COLOR_MISS = QColor('#16a34a')

STYLE_SHEET = """
QWidget { font-family: 'Microsoft YaHei UI', 'Segoe UI', sans-serif; font-size: 13px; }
#centralWidget { background: #f5f6f8; }
QLabel { background: transparent; color: #1f2937; }
QLabel#title { font-size: 20px; font-weight: 700; color: #111827; }
QLabel#subtitle { font-size: 12px; color: #6b7280; }
QLabel#hint { font-size: 12px; color: #9ca3af; }
QGroupBox {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    margin-top: 16px;
    padding: 14px 12px 10px 12px;
    font-weight: 600;
    color: #6b7280;
}
QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }
QRadioButton { background: transparent; font-weight: normal; color: #1f2937; }
/* 默认指示器在白色分组框上对比度太低，改为深色描边框，选中时填充蓝色 */
QCheckBox {
    background: transparent;
    font-weight: normal;
    color: #1f2937;
    spacing: 6px;
}
QCheckBox::indicator {
    width: 15px;
    height: 15px;
    border: 1px solid #6b7280;
    border-radius: 3px;
    background: #ffffff;
}
QCheckBox::indicator:hover { border-color: #2563eb; }
QCheckBox::indicator:checked { background: #2563eb; border-color: #1d4ed8; }
QCheckBox::indicator:disabled { border-color: #d1d5db; background: #f9fafb; }
QCheckBox::indicator:checked:disabled { background: #93c5fd; border-color: #93c5fd; }
QCheckBox:checked { font-weight: 600; }
QComboBox, QDoubleSpinBox {
    background: #ffffff;
    border: 1px solid #d1d5db;
    border-radius: 6px;
    padding: 4px 6px;
    font-weight: normal;
    color: #111827;
}
QComboBox:disabled, QDoubleSpinBox:disabled { background: #f9fafb; color: #9ca3af; }
QComboBox QAbstractItemView { background: #ffffff; color: #111827; selection-background-color: #bfdbfe; }
QLineEdit, QPlainTextEdit {
    background: #ffffff;
    border: 1px solid #d1d5db;
    border-radius: 6px;
    padding: 6px 8px;
    font-weight: normal;
    color: #111827;
    selection-background-color: #bfdbfe;
}
QLineEdit:focus, QPlainTextEdit:focus { border: 1px solid #2563eb; }
QPlainTextEdit#log { font-family: Consolas, 'Courier New', monospace; font-size: 9pt; }
QScrollArea#upperScroll, QScrollArea#upperScroll > QWidget { background: transparent; border: none; }
QSplitter::handle { background: transparent; }
QSplitter::handle:vertical { height: 8px; }
QSplitter::handle:hover { background: #dbeafe; border-radius: 4px; }
QLineEdit:disabled, QPlainTextEdit:disabled { background: #f9fafb; color: #9ca3af; }
QPushButton {
    background: #ffffff;
    border: 1px solid #d1d5db;
    border-radius: 6px;
    padding: 6px 14px;
    font-weight: normal;
    color: #1f2937;
}
QPushButton:hover { background: #f3f4f6; }
QPushButton:pressed { background: #e5e7eb; }
QPushButton:disabled { color: #9ca3af; background: #f9fafb; border-color: #e5e7eb; }
QPushButton#primary {
    background: #2563eb;
    border: 1px solid #2563eb;
    color: #ffffff;
    font-weight: 600;
}
QPushButton#primary:hover { background: #1d4ed8; }
QPushButton#primary:disabled { background: #bfdbfe; border-color: #bfdbfe; color: #f8fafc; }
QStatusBar { background: #eef0f3; color: #4b5563; }
"""


def python_exe():
    """返回用于启动脚本的 python 解释器（pythonw 无法接收控制台输出）。"""
    exe = Path(sys.executable)
    if exe.name.lower().startswith('pythonw'):
        candidate = exe.with_name('python.exe')
        if candidate.exists():
            return str(candidate)
    return str(exe)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('财务指标分析工具')
        self.resize(920, 760)

        self._process = None
        self._tmp_dir = None
        # 阈值规则：[ (正则, 比较方式, 阈值) ]，在每次开始运行时生成
        self._rules = []
        # 子进程输出可能被拆成半行，先缓冲到换行再着色
        self._line_buf = ''
        self._rule_rows = []

        self._build_ui()
        self._load_settings()  # 恢复上次界面参数
        self._sync_source_enabled()
        self.statusBar().showMessage('就绪')

    # ---------------- 界面搭建 ----------------
    def _build_ui(self):
        central = QWidget()
        central.setObjectName('centralWidget')
        self.setCentralWidget(central)

        root = QVBoxLayout(central)
        root.setContentsMargins(20, 18, 20, 16)
        root.setSpacing(12)

        title = QLabel('财务指标分析工具')
        title.setObjectName('title')
        subtitle = QLabel('A 股 / 港股财务指标抓取与汇总，数据来源 akshare')
        subtitle.setObjectName('subtitle')
        root.addWidget(title)
        root.addWidget(subtitle)

        # 参数区域整体作为一个控件，便于与日志区放入分隔器
        upper = QWidget()
        upper_layout = QVBoxLayout(upper)
        upper_layout.setContentsMargins(0, 0, 0, 0)
        upper_layout.setSpacing(12)
        upper_layout.addWidget(self._build_source_box())
        upper_layout.addWidget(self._build_output_box())
        upper_layout.addWidget(self._build_rule_box())
        upper_layout.addLayout(self._build_action_row())

        # 参数区放入滚动区，使其可被压缩，日志区才能向上扩展
        upper_scroll = QScrollArea()
        upper_scroll.setObjectName('upperScroll')
        upper_scroll.setWidgetResizable(True)
        upper_scroll.setWidget(upper)
        upper_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        upper_scroll.setMinimumHeight(160)
        # 视口不填充背景，避免滚动区出现白色底板
        upper_scroll.viewport().setAutoFillBackground(False)
        upper.setAutoFillBackground(False)

        # 垂直分隔器：拖动分隔条即可调节日志窗口高度
        self.splitter = QSplitter(Qt.Vertical)
        self.splitter.setChildrenCollapsible(False)
        self.splitter.setHandleWidth(8)
        self.splitter.addWidget(upper_scroll)
        self.splitter.addWidget(self._build_log_box())
        self.splitter.setStretchFactor(0, 0)
        self.splitter.setStretchFactor(1, 1)
        self.splitter.setSizes([470, 260])
        root.addWidget(self.splitter, 1)

    def _build_source_box(self):
        box = QGroupBox('数据来源')
        layout = QVBoxLayout(box)
        layout.setSpacing(8)

        radio_row = QHBoxLayout()
        self.rb_file = QRadioButton('使用配置文件')
        self.rb_manual = QRadioButton('手动输入股票代码')
        self.rb_file.setChecked(True)
        group = QButtonGroup(self)
        group.addButton(self.rb_file)
        group.addButton(self.rb_manual)
        group.buttonToggled.connect(self._sync_source_enabled)
        radio_row.addWidget(self.rb_file)
        radio_row.addWidget(self.rb_manual)
        radio_row.addStretch(1)
        layout.addLayout(radio_row)

        file_row = QHBoxLayout()
        self.cfg_edit = QLineEdit(str(DEFAULT_CONFIG))
        self.cfg_edit.setPlaceholderText('选择包含股票代码的文本文件，每行一个代码')
        self.btn_cfg = QPushButton('浏览...')
        self.btn_cfg.clicked.connect(self._browse_config)
        file_row.addWidget(QLabel('配置文件'))
        file_row.addWidget(self.cfg_edit, 1)
        file_row.addWidget(self.btn_cfg)
        layout.addLayout(file_row)

        self.codes_edit = QPlainTextEdit()
        self.codes_edit.setPlaceholderText('每行一个股票代码，例如：\n002600    （6 位为 A 股）\n00700     （少于 6 位为港股）')
        self.codes_edit.setFixedHeight(88)
        layout.addWidget(self.codes_edit)

        return box

    def _build_output_box(self):
        box = QGroupBox('输出设置')
        layout = QVBoxLayout(box)
        layout.setSpacing(8)

        out_row = QHBoxLayout()
        self.out_edit = QLineEdit(str(DEFAULT_OUTPUT))
        self.btn_out = QPushButton('浏览...')
        self.btn_out.clicked.connect(self._browse_output)
        self.btn_open = QPushButton('打开目录')
        self.btn_open.clicked.connect(self._open_output)
        out_row.addWidget(QLabel('输出目录'))
        out_row.addWidget(self.out_edit, 1)
        out_row.addWidget(self.btn_out)
        out_row.addWidget(self.btn_open)
        layout.addLayout(out_row)

        hint = QLabel('个股结果保存为 <代码>.csv；文件已存在时直接复用缓存，不再重复下载')
        hint.setObjectName('hint')
        layout.addWidget(hint)

        return box

    def _build_rule_box(self):
        """指标阈值设置区：勾选后启用，命中阈值红色、未命中绿色。"""
        box = QGroupBox('指标阈值设置')
        layout = QVBoxLayout(box)
        layout.setSpacing(8)

        grid = QGridLayout()
        grid.setHorizontalSpacing(18)
        grid.setVerticalSpacing(6)
        self._rule_rows = []
        for index, name in enumerate(RULE_ITEMS):
            row, col = divmod(index, 2)
            cell = QHBoxLayout()
            cell.setSpacing(6)

            chk = QCheckBox(name)
            # 勾选后标签加粗，留足宽度避免“应收账款周转率(次)”被截断
            chk.setMinimumWidth(155)
            mode = QComboBox()
            mode.addItems(['大于', '小于'])
            mode.setFixedWidth(72)
            value = QDoubleSpinBox()
            value.setRange(-1e9, 1e9)
            value.setDecimals(2)
            value.setSingleStep(0.5)
            value.setFixedWidth(96)
            chk.toggled.connect(
                lambda checked, m=mode, v=value: self._sync_rule_row(checked, m, v)
            )
            self._sync_rule_row(False, mode, value)

            cell.addWidget(chk)
            cell.addWidget(mode)
            cell.addWidget(value)
            cell.addStretch(1)
            grid.addLayout(cell, row, col)
            self._rule_rows.append((name, chk, mode, value))

        layout.addLayout(grid)

        hint = QLabel('勾选后生效：指标结果满足条件显示红色，否则显示绿色；未勾选的指标保持默认颜色')
        hint.setObjectName('hint')
        layout.addWidget(hint)
        return box

    @staticmethod
    def _sync_rule_row(checked, mode, value):
        """勾选状态决定比较方式与阈值输入是否可用。"""
        mode.setEnabled(checked)
        value.setEnabled(checked)

    def _build_action_row(self):
        row = QHBoxLayout()
        row.setSpacing(8)

        self.btn_start = QPushButton('开始分析')
        self.btn_start.setObjectName('primary')
        self.btn_start.setMinimumWidth(120)
        self.btn_start.clicked.connect(self._start)

        self.btn_stop = QPushButton('停止')
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self._stop)

        self.btn_clear = QPushButton('清空日志')
        self.btn_clear.clicked.connect(self._clear_log)

        self.state_label = QLabel('未运行')
        self.state_label.setObjectName('hint')

        row.addWidget(self.btn_start)
        row.addWidget(self.btn_stop)
        row.addWidget(self.btn_clear)
        row.addStretch(1)
        row.addWidget(self.state_label)
        return row

    def _build_log_box(self):
        box = QGroupBox('运行日志')
        layout = QVBoxLayout(box)
        self.log = QPlainTextEdit()
        self.log.setObjectName('log')
        self.log.setReadOnly(True)
        self.log.setFont(QFont('Consolas', 9))
        self.log.setMaximumBlockCount(20000)
        self.log.setMinimumHeight(100)
        layout.addWidget(self.log)
        return box

    # ---------------- 交互逻辑 ----------------
    def _sync_source_enabled(self, *args):
        use_file = self.rb_file.isChecked()
        for widget in (self.cfg_edit, self.btn_cfg):
            widget.setEnabled(use_file)
        self.codes_edit.setEnabled(not use_file)

    def _browse_config(self):
        start = self.cfg_edit.text().strip() or str(SCRIPT_DIR)
        path, _ = QFileDialog.getOpenFileName(
            self, '选择配置文件', start, '文本文件 (*.ini *.txt *.csv);;所有文件 (*.*)'
        )
        if path:
            self.cfg_edit.setText(path)

    def _browse_output(self):
        start = self.out_edit.text().strip() or str(SCRIPT_DIR)
        path = QFileDialog.getExistingDirectory(self, '选择输出目录', start)
        if path:
            self.out_edit.setText(path)

    def _open_output(self):
        out_dir = Path(self.out_edit.text().strip() or str(DEFAULT_OUTPUT))
        out_dir.mkdir(parents=True, exist_ok=True)
        # 优先使用系统默认方式打开目录（Windows 下为资源管理器）
        if not QDesktopServices.openUrl(QUrl.fromLocalFile(str(out_dir))):
            QMessageBox.warning(self, '无法打开', f'打开目录失败：{out_dir}')

    # ---------------- 界面参数持久化 ----------------
    def _load_settings(self):
        """读取 setting.ini 恢复上次界面参数；文件缺失或内容异常时保持默认值。"""
        parser = configparser.ConfigParser(interpolation=None)
        try:
            if not SETTINGS_PATH.is_file():
                return
            parser.read(SETTINGS_PATH, encoding='utf-8')
        except (OSError, configparser.Error):
            return

        ui = parser['ui'] if parser.has_section('ui') else {}
        if ui.get('source', 'file') == 'manual':
            self.rb_manual.setChecked(True)
        if ui.get('config'):
            self.cfg_edit.setText(ui['config'])
        if ui.get('output'):
            self.out_edit.setText(ui['output'])
        codes = [c.strip() for c in ui.get('codes', '').split(',') if c.strip()]
        if codes:
            self.codes_edit.setPlainText('\n'.join(codes))

        rules = parser['thresholds'] if parser.has_section('thresholds') else {}
        for name, chk, mode, value in self._rule_rows:
            parts = [p.strip() for p in rules.get(name, '').split(',')]
            if len(parts) != 3:
                continue
            try:
                threshold = float(parts[2])
            except ValueError:
                continue
            chk.setChecked(parts[0] in ('1', 'on', 'true'))
            mode.setCurrentIndex(1 if parts[1] == 'lt' else 0)
            value.setValue(threshold)

    def _save_settings(self):
        """把当前界面参数写入 setting.ini；写入失败只提示，不影响使用。"""
        parser = configparser.ConfigParser(interpolation=None)
        codes = [line.strip() for line in self.codes_edit.toPlainText().splitlines() if line.strip()]
        parser['ui'] = {
            'source': 'manual' if self.rb_manual.isChecked() else 'file',
            'config': self.cfg_edit.text().strip(),
            'output': self.out_edit.text().strip(),
            'codes': ','.join(codes),
        }
        rules = {}
        for name, chk, mode, value in self._rule_rows:
            enabled = '1' if chk.isChecked() else '0'
            compare = 'gt' if mode.currentIndex() == 0 else 'lt'
            rules[name] = f'{enabled},{compare},{value.value():g}'
        parser['thresholds'] = rules
        try:
            with open(SETTINGS_PATH, 'w', encoding='utf-8') as file:
                parser.write(file)
        except OSError as exc:
            self.statusBar().showMessage(f'保存设置失败：{exc}')

    # ---------------- 运行控制 ----------------
    def _collect_codes(self):
        """从手动输入读取股票代码，返回 (codes, 错误信息)。"""
        codes = []
        for raw in self.codes_edit.toPlainText().splitlines():
            code = raw.strip()
            if not code:
                continue
            if not (code.isdigit() and len(code) <= 6):
                return None, f'股票代码不合法：{code}（应为不超过 6 位的数字）'
            codes.append(code)
        if not codes:
            return None, '请至少输入一个股票代码'
        return codes, None

    def _prepare_config(self):
        """按当前模式准备配置文件路径，返回 (path, 错误信息)。"""
        if self.rb_file.isChecked():
            path = Path(self.cfg_edit.text().strip())
            if not path.is_file():
                return None, f'配置文件不存在：{path}'
            if not path.read_text(encoding='utf-8').strip():
                return None, '配置文件内容为空'
            return path, None

        codes, err = self._collect_codes()
        if err:
            return None, err
        self._cleanup_tmp()
        self._tmp_dir = tempfile.mkdtemp(prefix='baobiao_ui_')
        tmp_file = Path(self._tmp_dir) / 'codes.ini'
        # 与 config.ini 保持一致：每行一个代码
        tmp_file.write_text('\n'.join(codes) + '\n', encoding='utf-8')
        return tmp_file, None

    def _start(self):
        if self._process is not None:
            return
        if not SCRIPT_PATH.is_file():
            QMessageBox.critical(self, '缺少脚本', f'未找到 {SCRIPT_PATH}')
            return

        config_path, err = self._prepare_config()
        if err:
            QMessageBox.warning(self, '参数有误', err)
            return

        out_dir = Path(self.out_edit.text().strip() or str(DEFAULT_OUTPUT))
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            QMessageBox.warning(self, '参数有误', f'创建输出目录失败：{exc}')
            return

        self._save_settings()  # 校验通过后再落盘，避免保存无效参数
        self.log.clear()
        self._line_buf = ''
        self._rules = self._collect_rules()  # 阈值为一次性快照，运行中不允许改动
        self._append(f'配置文件：{config_path}\n')
        self._append(f'输出目录：{out_dir}\n')
        self._append('-' * 100 + '\n')

        self._process = QProcess(self)
        self._process.setWorkingDirectory(str(SCRIPT_DIR))
        env = QProcessEnvironment.systemEnvironment()
        env.insert('PYTHONIOENCODING', 'utf-8')  # 统一子进程输出编码，避免中文乱码
        env.insert('PYTHONUNBUFFERED', '1')
        self._process.setProcessEnvironment(env)
        self._process.readyReadStandardOutput.connect(self._read_stdout)
        self._process.readyReadStandardError.connect(self._read_stderr)
        self._process.finished.connect(self._on_finished)
        self._process.errorOccurred.connect(self._on_process_error)

        program = python_exe()
        arguments = ['-u', str(SCRIPT_PATH), '-f', str(config_path), '-o', str(out_dir)]
        self._process.start(program, arguments)

        self._set_running(True)

    def _stop(self):
        if self._process is None:
            return
        self._process.kill()
        self._append('\n[已手动停止]\n')

    def _read_stdout(self):
        text = bytes(self._process.readAllStandardOutput()).decode('utf-8', 'replace')
        self._feed_output(text)

    def _read_stderr(self):
        text = bytes(self._process.readAllStandardError()).decode('utf-8', 'replace')
        if text:
            self._append(''.join(f'[stderr] {line}\n' for line in text.splitlines() if line.strip()))

    def _on_finished(self, code=0, status=None):
        self._append(f'\n[进程结束] 退出码 {code}\n')
        self._process = None
        self._set_running(False)
        self._cleanup_tmp()
        self.statusBar().showMessage('分析完成' if code == 0 else f'分析结束（退出码 {code}）')

    def _on_process_error(self, error=None):
        self._append(f'\n[启动失败] {error}\n')
        self._process = None
        self._set_running(False)
        self._cleanup_tmp()
        self.statusBar().showMessage('启动失败')

    def _set_running(self, running):
        self.btn_start.setEnabled(not running)
        self.btn_stop.setEnabled(running)
        self.state_label.setText('运行中...' if running else '未运行')
        for widget in (self.rb_file, self.rb_manual, self.cfg_edit, self.btn_cfg,
                       self.codes_edit, self.out_edit, self.btn_out):
            widget.setEnabled(not running)
        # 阈值设置仅在空闲时可改，避免与实际生效规则不一致
        for _name, chk, mode, value in self._rule_rows:
            chk.setEnabled(not running)
            self._sync_rule_row(not running and chk.isChecked(), mode, value)
        if not running:
            self._sync_source_enabled()
        self.statusBar().showMessage('正在运行...' if running else '就绪')

    # ---------------- 日志与清理 ----------------
    def _collect_rules(self):
        """读取界面设置，生成 [(行首正则, 'gt'/'lt', 阈值)]。"""
        rules = []
        for name, chk, mode, value in self._rule_rows:
            if not chk.isChecked():
                continue
            # 数值允许负数与科学计数法；名称后可能直接跟数字（如存货周转评分）
            pattern = re.compile(
                rf'^{re.escape(name)}\s*(-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)'
            )
            rules.append((pattern, 'gt' if mode.currentIndex() == 0 else 'lt', value.value()))
        return rules

    def _line_color(self, line):
        """按阈值规则返回该行颜色，未命中任何规则时返回 None（默认颜色）。"""
        text = line.rstrip('\r')
        for pattern, mode, threshold in self._rules:
            match = pattern.match(text)
            if not match:
                continue
            try:
                number = float(match.group(1))
            except ValueError:
                return None
            hit = number > threshold if mode == 'gt' else number < threshold
            return COLOR_HIT if hit else COLOR_MISS
        return None

    def _insert_text(self, text, color=None):
        if not text:
            return
        # 始终显式指定格式，否则默认文本会继承上一行的颜色
        fmt = QTextCharFormat()
        if color is not None:
            fmt.setForeground(color)
        cursor = self.log.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log.setTextCursor(cursor)
        cursor.insertText(text, fmt)
        self.log.ensureCursorVisible()

    def _feed_output(self, text):
        """按行接收子进程输出：完整行着色，半行先缓冲。"""
        if not text:
            return
        self._line_buf += text
        *lines, self._line_buf = self._line_buf.split('\n')
        for line in lines:
            self._insert_text(line + '\n', self._line_color(line))

    def _flush_line_buf(self):
        """把未以换行结束的残留内容按默认颜色输出。"""
        if self._line_buf:
            text, self._line_buf = self._line_buf, ''
            self._insert_text(text)

    def _append(self, text, color=None):
        self._flush_line_buf()
        self._insert_text(text, color)

    def _clear_log(self):
        self._line_buf = ''
        self.log.clear()

    def _cleanup_tmp(self):
        if self._tmp_dir:
            shutil.rmtree(self._tmp_dir, ignore_errors=True)
            self._tmp_dir = None

    def closeEvent(self, event):
        if self._process is not None:
            self._process.kill()
            self._process.waitForFinished(2000)
        self._save_settings()  # 关闭时保存，未运行也能记住当前设置
        self._cleanup_tmp()
        super().closeEvent(event)


def main():
    if QT_BINDING == 'PyQt5':
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    app = QApplication(sys.argv)
    app.setApplicationName('财务指标分析工具')
    app.setStyleSheet(STYLE_SHEET)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_() if QT_BINDING == 'PyQt5' else app.exec())


if __name__ == '__main__':
    main()
