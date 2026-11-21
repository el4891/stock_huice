"""统计套利（配对交易）回测工具的 Qt 图形界面。

把 tongjitaoli.py 的命令行参数（运行次数、数据目录、运行间隔、数据周期）
做成界面选项，以子进程方式调用原脚本，并实时显示运行日志。
依赖：PyQt5（未安装时自动回退到 PySide6）。
"""

import re
import sys
from pathlib import Path

# Qt 绑定兼容导入：优先 PyQt5，其次 PySide6
try:
    from PyQt5.QtCore import QProcess, QProcessEnvironment, Qt, QUrl
    from PyQt5.QtGui import QDesktopServices, QFont, QTextCursor
    from PyQt5.QtWidgets import (
        QApplication,
        QComboBox,
        QFileDialog,
        QGroupBox,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMessageBox,
        QPlainTextEdit,
        QPushButton,
        QScrollArea,
        QSpinBox,
        QSplitter,
        QVBoxLayout,
        QWidget,
    )

    QT_BINDING = 'PyQt5'
except ImportError:
    from PySide6.QtCore import QProcess, QProcessEnvironment, Qt, QUrl
    from PySide6.QtGui import QDesktopServices, QFont, QTextCursor
    from PySide6.QtWidgets import (
        QApplication,
        QComboBox,
        QFileDialog,
        QGroupBox,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMessageBox,
        QPlainTextEdit,
        QPushButton,
        QScrollArea,
        QSpinBox,
        QSplitter,
        QVBoxLayout,
        QWidget,
    )

    QT_BINDING = 'PySide6'

SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPT_PATH = SCRIPT_DIR / 'tongjitaoli.py'
CONFIG_PATH = SCRIPT_DIR / 'config.ini'
DEFAULT_DATA_DIR = SCRIPT_DIR / 'data_sina'

# 脚本使用 colorama 输出颜色码，界面中需去掉转义序列
ANSI_RE = re.compile(r'\x1b\[[0-9;?]*[ -/]*[@-~]')

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
QLineEdit, QPlainTextEdit, QSpinBox, QComboBox {
    background: #ffffff;
    border: 1px solid #d1d5db;
    border-radius: 6px;
    padding: 6px 8px;
    font-weight: normal;
    color: #111827;
    selection-background-color: #bfdbfe;
}
QLineEdit:focus, QPlainTextEdit:focus, QSpinBox:focus, QComboBox:focus { border: 1px solid #2563eb; }
QLineEdit:disabled, QPlainTextEdit:disabled, QSpinBox:disabled, QComboBox:disabled {
    background: #f9fafb; color: #9ca3af;
}
QComboBox QAbstractItemView {
    background: #ffffff;
    color: #111827;
    border: 1px solid #d1d5db;
    selection-background-color: #dbeafe;
    selection-color: #111827;
    outline: none;
}
QComboBox QAbstractItemView::item { color: #111827; padding: 4px 8px; }
QComboBox QAbstractItemView::item:selected { color: #111827; background: #dbeafe; }
QPlainTextEdit#log { font-family: Consolas, 'Courier New', monospace; font-size: 9pt; }
QScrollArea#upperScroll, QScrollArea#upperScroll > QWidget { background: transparent; border: none; }
QSplitter::handle { background: transparent; }
QSplitter::handle:vertical { height: 8px; }
QSplitter::handle:hover { background: #dbeafe; border-radius: 4px; }
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
        self.setWindowTitle('统计套利回测工具')
        self.resize(920, 720)

        self._process = None

        self._build_ui()
        self._sync_interval_enabled()
        self.statusBar().showMessage('就绪')

    # ---------------- 界面搭建 ----------------
    def _build_ui(self):
        central = QWidget()
        central.setObjectName('centralWidget')
        self.setCentralWidget(central)

        root = QVBoxLayout(central)
        root.setContentsMargins(20, 18, 20, 16)
        root.setSpacing(12)

        title = QLabel('统计套利回测工具')
        title.setObjectName('title')
        subtitle = QLabel('配对交易价差回测（backtrader），交易对与窗口参数读取自 config.ini')
        subtitle.setObjectName('subtitle')
        root.addWidget(title)
        root.addWidget(subtitle)

        # 参数区整体放入滚动区，压缩后仍可查看全部选项
        upper = QWidget()
        upper_layout = QVBoxLayout(upper)
        upper_layout.setContentsMargins(0, 0, 0, 0)
        upper_layout.setSpacing(12)
        upper_layout.addWidget(self._build_param_box())
        upper_layout.addWidget(self._build_config_box())
        upper_layout.addLayout(self._build_action_row())

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
        self.splitter.setSizes([330, 300])
        root.addWidget(self.splitter, 1)

    def _build_param_box(self):
        box = QGroupBox('运行参数')
        layout = QVBoxLayout(box)
        layout.setSpacing(8)

        row1 = QHBoxLayout()
        row1.setSpacing(8)
        self.combo_period = QComboBox()
        self.combo_period.addItems(['60 分钟线', '日线'])
        self.spin_times = QSpinBox()
        self.spin_times.setRange(1, 9999)
        self.spin_times.setValue(1)
        self.spin_times.setFixedWidth(100)
        self.spin_sleep = QSpinBox()
        self.spin_sleep.setRange(0, 86400)
        self.spin_sleep.setValue(300)
        self.spin_sleep.setSuffix(' 秒')
        self.spin_sleep.setFixedWidth(130)

        row1.addWidget(QLabel('数据周期'))
        row1.addWidget(self.combo_period)
        row1.addSpacing(12)
        row1.addWidget(QLabel('运行次数'))
        row1.addWidget(self.spin_times)
        row1.addSpacing(12)
        row1.addWidget(QLabel('运行间隔'))
        row1.addWidget(self.spin_sleep)
        row1.addStretch(1)
        layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.setSpacing(8)
        self.dir_edit = QLineEdit(str(DEFAULT_DATA_DIR))
        self.dir_edit.setPlaceholderText('行情数据缓存目录，交易时段内脚本会先清空该目录')
        self.btn_dir = QPushButton('浏览...')
        self.btn_dir.clicked.connect(self._browse_data_dir)
        self.btn_open_dir = QPushButton('打开目录')
        self.btn_open_dir.clicked.connect(self._open_data_dir)
        row2.addWidget(QLabel('数据目录'))
        row2.addWidget(self.dir_edit, 1)
        row2.addWidget(self.btn_dir)
        row2.addWidget(self.btn_open_dir)
        layout.addLayout(row2)

        hint = QLabel('运行间隔仅在运行次数大于 1 时生效；数据周期对应脚本的 -l/--daily 参数')
        hint.setObjectName('hint')
        layout.addWidget(hint)

        self.spin_times.valueChanged.connect(self._sync_interval_enabled)
        return box

    def _build_config_box(self):
        box = QGroupBox('回测配置')
        layout = QVBoxLayout(box)
        layout.setSpacing(8)

        row = QHBoxLayout()
        row.setSpacing(8)
        self.cfg_edit = QLineEdit(str(CONFIG_PATH))
        self.cfg_edit.setReadOnly(True)
        self.btn_open_cfg = QPushButton('打开配置文件')
        self.btn_open_cfg.clicked.connect(self._open_config)
        row.addWidget(QLabel('配置文件'))
        row.addWidget(self.cfg_edit, 1)
        row.addWidget(self.btn_open_cfg)
        layout.addLayout(row)

        hint = QLabel('window / entry / exit / futures_list 需在配置文件中修改，修改后重新开始回测')
        hint.setObjectName('hint')
        layout.addWidget(hint)
        return box

    def _build_action_row(self):
        row = QHBoxLayout()
        row.setSpacing(8)

        self.btn_start = QPushButton('开始回测')
        self.btn_start.setObjectName('primary')
        self.btn_start.setMinimumWidth(120)
        self.btn_start.clicked.connect(self._start)

        self.btn_stop = QPushButton('停止')
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self._stop)

        self.btn_clear = QPushButton('清空日志')
        self.btn_clear.clicked.connect(lambda: self.log.clear())

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
    def _sync_interval_enabled(self, *args):
        # 单次运行时脚本不会 sleep，间隔无意义
        self.spin_sleep.setEnabled(self.spin_times.value() > 1)

    def _browse_data_dir(self):
        start = self.dir_edit.text().strip() or str(SCRIPT_DIR)
        path = QFileDialog.getExistingDirectory(self, '选择数据目录', start)
        if path:
            self.dir_edit.setText(path)

    def _open_data_dir(self):
        data_dir = Path(self.dir_edit.text().strip() or str(DEFAULT_DATA_DIR))
        try:
            data_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            QMessageBox.warning(self, '无法创建', f'创建数据目录失败：{exc}')
            return
        self._open_path(data_dir)

    def _open_config(self):
        if not CONFIG_PATH.is_file():
            QMessageBox.warning(self, '缺少配置', f'未找到 {CONFIG_PATH}')
            return
        self._open_path(CONFIG_PATH)

    @staticmethod
    def _open_path(path):
        """用系统默认程序打开文件或目录，失败时返回 False。"""
        return QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))

    # ---------------- 运行控制 ----------------
    def _start(self):
        if self._process is not None:
            return
        if not SCRIPT_PATH.is_file():
            QMessageBox.critical(self, '缺少脚本', f'未找到 {SCRIPT_PATH}')
            return
        if not CONFIG_PATH.is_file():
            QMessageBox.warning(self, '缺少配置', f'未找到配置文件 {CONFIG_PATH}')
            return

        data_dir = Path(self.dir_edit.text().strip() or str(DEFAULT_DATA_DIR))
        try:
            data_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            QMessageBox.warning(self, '参数有误', f'创建数据目录失败：{exc}')
            return

        times = self.spin_times.value()
        interval = self.spin_sleep.value()
        use_daily = self.combo_period.currentIndex() == 1

        self.log.clear()
        self._append(f'数据周期：{"日线" if use_daily else "60 分钟线"}\n')
        self._append(f'运行次数：{times}，运行间隔：{interval} 秒\n')
        self._append(f'数据目录：{data_dir}\n')
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
        arguments = [
            '-u', str(SCRIPT_PATH),
            '-t', str(times),
            '-d', str(data_dir),
            '-s', str(interval),
            '-l', 'true' if use_daily else 'false',
        ]
        self._process.start(program, arguments)

        self._set_running(True)

    def _stop(self):
        if self._process is None:
            return
        self._process.kill()
        self._append('\n[已手动停止]\n')

    def _read_stdout(self):
        text = bytes(self._process.readAllStandardOutput()).decode('utf-8', 'replace')
        self._append(text)

    def _read_stderr(self):
        text = bytes(self._process.readAllStandardError()).decode('utf-8', 'replace')
        if text:
            self._append(''.join(f'[stderr] {line}\n' for line in text.splitlines() if line.strip()))

    def _on_finished(self, code=0, status=None):
        self._append(f'\n[进程结束] 退出码 {code}\n')
        self._process = None
        self._set_running(False)
        self.statusBar().showMessage('回测完成' if code == 0 else f'回测结束（退出码 {code}）')

    def _on_process_error(self, error=None):
        self._append(f'\n[启动失败] {error}\n')
        self._process = None
        self._set_running(False)
        self.statusBar().showMessage('启动失败')

    def _set_running(self, running):
        self.btn_start.setEnabled(not running)
        self.btn_stop.setEnabled(running)
        self.state_label.setText('运行中...' if running else '未运行')
        for widget in (self.combo_period, self.spin_times, self.spin_sleep,
                       self.dir_edit, self.btn_dir, self.btn_open_dir, self.btn_open_cfg):
            widget.setEnabled(not running)
        if not running:
            self._sync_interval_enabled()
        self.statusBar().showMessage('正在回测...' if running else '就绪')

    # ---------------- 日志 ----------------
    def _append(self, text):
        if not text:
            return
        text = ANSI_RE.sub('', text)  # 去掉 colorama 颜色码
        cursor = self.log.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log.setTextCursor(cursor)
        self.log.insertPlainText(text)
        self.log.ensureCursorVisible()

    def closeEvent(self, event):
        if self._process is not None:
            self._process.kill()
            self._process.waitForFinished(2000)
        super().closeEvent(event)


def main():
    if QT_BINDING == 'PyQt5':
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    app = QApplication(sys.argv)
    app.setApplicationName('统计套利回测工具')
    app.setStyleSheet(STYLE_SHEET)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_() if QT_BINDING == 'PyQt5' else app.exec())


if __name__ == '__main__':
    main()
