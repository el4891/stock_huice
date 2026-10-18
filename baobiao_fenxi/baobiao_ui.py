"""财务指标分析工具的 Qt 图形界面。

把 baobiaofenxi.py 的命令行参数（配置文件、输出目录）做成界面选项，
以子进程方式调用原脚本，并实时显示运行日志。
依赖：PyQt5（未安装时自动回退到 PySide6）。
"""

import shutil
import sys
import tempfile
from pathlib import Path

# Qt 绑定兼容导入：优先 PyQt5，其次 PySide6
try:
    from PyQt5.QtCore import QProcess, QProcessEnvironment, Qt, QUrl
    from PyQt5.QtGui import QDesktopServices, QFont, QTextCursor
    from PyQt5.QtWidgets import (
        QApplication,
        QButtonGroup,
        QFileDialog,
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
    from PySide6.QtGui import QDesktopServices, QFont, QTextCursor
    from PySide6.QtWidgets import (
        QApplication,
        QButtonGroup,
        QFileDialog,
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

        self._build_ui()
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
        self.splitter.setSizes([400, 300])
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

        self.log.clear()
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
        self._append(text)

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
        if not running:
            self._sync_source_enabled()
        self.statusBar().showMessage('正在运行...' if running else '就绪')

    # ---------------- 日志与清理 ----------------
    def _append(self, text):
        if not text:
            return
        cursor = self.log.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log.setTextCursor(cursor)
        self.log.insertPlainText(text)
        self.log.ensureCursorVisible()

    def _cleanup_tmp(self):
        if self._tmp_dir:
            shutil.rmtree(self._tmp_dir, ignore_errors=True)
            self._tmp_dir = None

    def closeEvent(self, event):
        if self._process is not None:
            self._process.kill()
            self._process.waitForFinished(2000)
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
