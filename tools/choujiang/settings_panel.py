"""奖项设置面板。"""
from __future__ import annotations

import math
from typing import Any

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from config_store import (
    DEFAULT_FONT_SIZE,
    MAX_COUNT,
    MAX_FONT_SIZE,
    MIN_COUNT,
    MIN_FONT_SIZE,
    calculate_percentages,
)


class SettingsPanel(QWidget):
    """编辑奖项数量、名称、权重和转盘字体大小。"""

    settings_changed = Signal(list, int)
    message_requested = Signal(str)

    def __init__(self, prizes: list[dict[str, Any]], font_size: int = DEFAULT_FONT_SIZE) -> None:
        super().__init__()
        self._prizes = [dict(item) for item in prizes]
        self._font_size = max(MIN_FONT_SIZE, min(MAX_FONT_SIZE, int(font_size)))
        self._loading = False
        self._init_ui()
        self._refresh_table()

        self.count_spin.valueChanged.connect(self._on_count_changed)
        self.font_spin.valueChanged.connect(self._on_font_size_changed)
        self.table.itemChanged.connect(self._on_item_changed)

    def _init_ui(self) -> None:
        """创建界面控件。"""
        layout = QVBoxLayout(self)

        title = QLabel("奖项设置")
        title.setStyleSheet("font-size: 15px; font-weight: bold;")
        layout.addWidget(title)

        count_layout = QHBoxLayout()
        count_layout.addWidget(QLabel("奖项数量："))
        self.count_spin = QSpinBox()
        self.count_spin.setRange(MIN_COUNT, MAX_COUNT)
        self.count_spin.setValue(len(self._prizes))
        self.count_spin.setSuffix(" 个")
        count_layout.addWidget(self.count_spin)
        count_layout.addStretch(1)
        layout.addLayout(count_layout)

        font_layout = QHBoxLayout()
        font_layout.addWidget(QLabel("转盘字体："))
        self.font_spin = QSpinBox()
        self.font_spin.setRange(MIN_FONT_SIZE, MAX_FONT_SIZE)
        self.font_spin.setValue(self._font_size)
        self.font_spin.setSuffix(" 号")
        font_layout.addWidget(self.font_spin)
        font_layout.addStretch(1)
        layout.addLayout(font_layout)

        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["奖项名称", "权重", "占比"])
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectItems)
        self.table.setAlternatingRowColors(True)
        layout.addWidget(self.table, 1)

        hint = QLabel("修改任意设置后会自动保存；权重越大，中奖概率越高。")
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #666;")
        layout.addWidget(hint)

    @staticmethod
    def _format_weight(value: float) -> str:
        """格式化权重显示。"""
        if float(value).is_integer():
            return str(int(value))
        return f"{value:g}"

    def _refresh_table(self) -> None:
        """按当前奖项重建表格显示。"""
        self._loading = True
        percentages = calculate_percentages(self._prizes)
        self.table.setRowCount(len(self._prizes))

        for row, prize in enumerate(self._prizes):
            name_item = QTableWidgetItem(str(prize["name"]))
            weight_item = QTableWidgetItem(self._format_weight(float(prize["weight"])))
            percent_item = QTableWidgetItem(f"{percentages[row]:.1f}%")
            percent_item.setFlags(percent_item.flags() & ~Qt.ItemFlag.ItemIsEditable)

            name_item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
            weight_item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignCenter)
            percent_item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignCenter)

            self.table.setItem(row, 0, name_item)
            self.table.setItem(row, 1, weight_item)
            self.table.setItem(row, 2, percent_item)

        self._loading = False

    def _emit_change(self) -> None:
        """通知外部配置已变化。"""
        self.settings_changed.emit(
            [dict(item) for item in self._prizes],
            self._font_size,
        )

    def _on_font_size_changed(self, font_size: int) -> None:
        """字体大小变化时通知外部。"""
        if self._loading:
            return
        self._font_size = int(font_size)
        self._emit_change()

    def _on_count_changed(self, count: int) -> None:
        """数量变化时增删奖项。"""
        if self._loading:
            return

        current = len(self._prizes)
        if count < current:
            self._prizes = self._prizes[:count]
        elif count > current:
            for index in range(current + 1, count + 1):
                self._prizes.append({"name": f"奖项{index}", "weight": 1.0})

        self._refresh_table()
        self._emit_change()

    def _on_item_changed(self, item: QTableWidgetItem) -> None:
        """处理名称或权重编辑。"""
        if self._loading:
            return

        row, column = item.row(), item.column()
        if row < 0 or row >= len(self._prizes):
            return

        if column == 0:
            name = item.text().strip()
            if not name:
                self._restore_item(item, str(self._prizes[row]["name"]))
                self.message_requested.emit("奖项名称不能为空")
                return
            self._prizes[row]["name"] = name

        elif column == 1:
            try:
                weight = float(item.text())
            except ValueError:
                weight = -1.0

            if not math.isfinite(weight) or weight < 0:
                self._restore_item(item, self._format_weight(float(self._prizes[row]["weight"])))
                self.message_requested.emit("权重必须是大于等于 0 的数字")
                return

            self._prizes[row]["weight"] = weight
            if all(float(prize["weight"]) <= 0 for prize in self._prizes):
                for prize in self._prizes:
                    prize["weight"] = 1.0
                self._refresh_table()
                self.message_requested.emit("所有权重为 0，已自动改为等权重")
                self._emit_change()
                return

        else:
            return

        self._refresh_table()
        self._emit_change()

    def _restore_item(self, item: QTableWidgetItem, text: str) -> None:
        """把非法输入恢复为原值。"""
        self._loading = True
        item.setText(text)
        self._loading = False

    def prizes(self) -> list[dict[str, Any]]:
        """返回当前奖项副本。"""
        return [dict(item) for item in self._prizes]
