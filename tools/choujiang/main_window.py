"""主窗口。"""
from __future__ import annotations

import random
from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QSplitter,
    QVBoxLayout,
    QWidget,
)

import config_store
from settings_panel import SettingsPanel
from wheel_widget import WheelWidget


class MainWindow(QMainWindow):
    """抽奖转盘应用主窗口。"""

    def __init__(self) -> None:
        super().__init__()
        config = config_store.load_config()
        self._prizes: list[dict[str, Any]] = config["prizes"]
        self._font_size: int = config["font_size"]
        self._settings_collapsed: bool = config["settings_collapsed"]
        self._saved_splitter_sizes: list[int] | None = None
        self.setWindowTitle("抽奖转盘")
        self.resize(1000, 650)
        self._init_ui()

    def _init_ui(self) -> None:
        """创建主界面并连接信号。"""
        central = QWidget()
        self.setCentralWidget(central)

        root_layout = QHBoxLayout(central)
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self.splitter.setCollapsible(0, False)
        self.splitter.setCollapsible(1, True)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)

        header_layout = QHBoxLayout()
        title = QLabel("抽奖转盘")
        title.setStyleSheet("font-size: 18px; font-weight: bold;")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header_layout.addWidget(title, 1)
        self.toggle_settings_button = QPushButton("收起设置")
        header_layout.addWidget(self.toggle_settings_button)
        left_layout.addLayout(header_layout)

        self.wheel = WheelWidget(self._prizes, self._font_size)
        left_layout.addWidget(self.wheel, 1)

        self.result_label = QLabel("准备抽奖")
        self.result_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.result_label.setStyleSheet(
            "font-size: 18px; font-weight: bold; color: #c0392b; padding: 6px;"
        )
        left_layout.addWidget(self.result_label)

        self.spin_button = QPushButton("开始抽奖")
        self.spin_button.setMinimumHeight(40)
        self.spin_button.setStyleSheet(
            "QPushButton { font-size: 15px; font-weight: bold; }"
        )
        left_layout.addWidget(self.spin_button)

        self.settings = SettingsPanel(self._prizes, self._font_size)
        self.splitter.addWidget(left_panel)
        self.splitter.addWidget(self.settings)
        self.splitter.setStretchFactor(0, 3)
        self.splitter.setStretchFactor(1, 2)
        root_layout.addWidget(self.splitter)

        self.toggle_settings_button.clicked.connect(self._toggle_settings)
        self.spin_button.clicked.connect(self.start_spin)
        self.wheel.spin_finished.connect(self._on_spin_finished)
        self.settings.settings_changed.connect(self._on_settings_changed)
        self.settings.message_requested.connect(self.statusBar().showMessage)
        self._apply_settings_visibility()

    def _toggle_settings(self) -> None:
        """收起或展开奖项设置面板。"""
        if not self._settings_collapsed:
            sizes = self.splitter.sizes()
            if len(sizes) == 2 and all(size > 0 for size in sizes):
                self._saved_splitter_sizes = sizes
            self.settings.hide()
            self._settings_collapsed = True
            self.toggle_settings_button.setText("展开设置")
        else:
            self.settings.show()
            sizes = self._saved_splitter_sizes
            if sizes and len(sizes) == 2 and all(size > 0 for size in sizes):
                self.splitter.setSizes(sizes)
            else:
                total = max(self.splitter.width(), 1)
                left = max(1, int(total * 0.6))
                self.splitter.setSizes([left, max(1, total - left)])
            self._settings_collapsed = False
            self.toggle_settings_button.setText("收起设置")

        self._save_settings()

    def _apply_settings_visibility(self) -> None:
        """按配置恢复设置面板显示状态，不触发保存。"""
        if self._settings_collapsed:
            sizes = self.splitter.sizes()
            if len(sizes) == 2 and all(size > 0 for size in sizes):
                self._saved_splitter_sizes = sizes
            self.settings.hide()
            self.toggle_settings_button.setText("展开设置")
        else:
            self.settings.show()
            self.toggle_settings_button.setText("收起设置")

    def _save_settings(self) -> None:
        """保存全部设置。"""
        try:
            config_store.save_config(
                self._prizes,
                self._font_size,
                self._settings_collapsed,
            )
            self.statusBar().showMessage("设置已自动保存", 3000)
        except OSError as error:
            self.statusBar().showMessage(f"保存失败：{error}", 5000)

    def _on_settings_changed(self, prizes: list[dict[str, Any]], font_size: int) -> None:
        """设置变化后更新转盘并自动保存。"""
        self._prizes = [dict(item) for item in prizes]
        self._font_size = int(font_size)
        self.wheel.set_prizes(self._prizes)
        self.wheel.set_font_size(self._font_size)
        self._save_settings()

    def start_spin(self) -> None:
        """按权重抽取奖项并开始旋转。"""
        if self.wheel.is_spinning():
            return

        weights = [float(item["weight"]) for item in self._prizes]
        if not weights or sum(weights) <= 0:
            self.statusBar().showMessage("请至少设置一个权重大于 0 的奖项", 5000)
            return

        index = random.choices(range(len(weights)), weights=weights, k=1)[0]
        self.result_label.setText("抽奖中...")
        self.spin_button.setEnabled(False)
        self.settings.setEnabled(False)
        self.statusBar().showMessage("正在抽奖...", 2000)
        self.wheel.spin_to_index(index)

    def _on_spin_finished(self, index: int) -> None:
        """显示抽奖结果并恢复界面状态。"""
        self.spin_button.setEnabled(True)
        self.settings.setEnabled(True)

        if 0 <= index < len(self._prizes):
            name = str(self._prizes[index]["name"])
            self.result_label.setText(f"中奖：{name}")
            self.statusBar().showMessage(f"本次抽中：{name}", 5000)
        else:
            self.result_label.setText("抽奖结束")
