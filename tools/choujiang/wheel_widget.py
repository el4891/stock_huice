"""抽奖转盘绘制与旋转动画。"""
from __future__ import annotations

import math
import random
from typing import Any

from PySide6.QtCore import (
    Property,
    QAbstractAnimation,
    Qt,
    QEasingCurve,
    QPointF,
    QPropertyAnimation,
    QRectF,
    QSize,
    Signal,
)
from PySide6.QtGui import QBrush, QColor, QFont, QPainter, QPen, QPolygonF
from PySide6.QtWidgets import QSizePolicy, QWidget

from config_store import DEFAULT_FONT_SIZE, MAX_FONT_SIZE, MIN_FONT_SIZE

COLORS = [
    "#FFB3BA",
    "#FFDFBA",
    "#FFFFBA",
    "#BAFFC9",
    "#BAE1FF",
    "#E6BAFF",
    "#FFC9DE",
    "#C9F0FF",
]


class WheelWidget(QWidget):
    """自绘抽奖转盘。"""

    spin_finished = Signal(int)

    def __init__(self, prizes: list[dict[str, Any]], font_size: int = DEFAULT_FONT_SIZE) -> None:
        super().__init__()
        self._prizes = [dict(item) for item in prizes]
        self._font_size = max(MIN_FONT_SIZE, min(MAX_FONT_SIZE, int(font_size)))
        self._rotation = 0.0
        self._animation: QPropertyAnimation | None = None
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    def get_rotation(self) -> float:
        """读取旋转角度。"""
        return self._rotation

    def set_rotation(self, value: float) -> None:
        """设置旋转角度并刷新绘制。"""
        self._rotation = float(value)
        self.update()

    rotation = Property(float, get_rotation, set_rotation)

    def minimumSizeHint(self) -> QSize:
        """返回最小尺寸。"""
        return QSize(420, 420)

    def set_prizes(self, prizes: list[dict[str, Any]]) -> None:
        """更新奖项并刷新转盘。"""
        self._prizes = [dict(item) for item in prizes]
        self.update()

    def set_font_size(self, font_size: int) -> None:
        """更新字体大小并刷新转盘。"""
        self._font_size = max(MIN_FONT_SIZE, min(MAX_FONT_SIZE, int(font_size)))
        self.update()

    def is_spinning(self) -> bool:
        """是否正在旋转。"""
        return (
            self._animation is not None
            and self._animation.state() == QAbstractAnimation.State.Running
        )

    def spin_to_index(self, index: int) -> None:
        """旋转并停在指定奖项。"""
        count = len(self._prizes)
        if count == 0 or index < 0 or index >= count or self.is_spinning():
            return

        segment_angle = 360.0 / count
        target_mod = (-(index * segment_angle + segment_angle / 2)) % 360
        current_mod = self._rotation % 360
        delta = (target_mod - current_mod) % 360
        end_value = self._rotation + 360 * random.randint(5, 8) + delta

        self._animation = QPropertyAnimation(self, b"rotation")
        self._animation.setDuration(random.randint(3500, 4500))
        self._animation.setStartValue(self._rotation)
        self._animation.setEndValue(end_value)
        self._animation.setEasingCurve(QEasingCurve.Type.OutCubic)
        self._animation.finished.connect(self._on_animation_finished)
        self._animation.start()

    def _on_animation_finished(self) -> None:
        """动画结束后计算指针指向的奖项。"""
        count = len(self._prizes)
        if count == 0:
            return

        angle_at_pointer = (-self._rotation) % 360
        index = int(angle_at_pointer / (360.0 / count)) % count
        self.spin_finished.emit(index)

    def paintEvent(self, event: Any) -> None:
        """绘制转盘、指针和中心圆。"""
        super().paintEvent(event)
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        width, height = self.width(), self.height()
        side = min(width, height) - 24
        if side <= 0:
            return

        wheel_rect = QRectF((width - side) / 2, (height - side) / 2, side, side)
        center = wheel_rect.center()
        count = len(self._prizes)
        if count == 0:
            return

        segment_angle = 360.0 / count

        # 先绘制所有扇区，避免后续扇区覆盖已经绘制的文字
        for index in range(count):
            start_units = round((90 + index * segment_angle + self._rotation) * 16)
            end_units = round((90 + (index + 1) * segment_angle + self._rotation) * 16)
            color = QColor(COLORS[index % len(COLORS)])
            painter.setBrush(QBrush(color))
            painter.setPen(QPen(QColor("#ffffff"), 2))
            painter.drawPie(wheel_rect, start_units, end_units - start_units)

        # 再绘制文字；少奖项水平居中，多奖项沿扇区中线放置避免跨扇区
        base_font = painter.font()
        label_font = QFont(base_font)
        label_font.setPointSize(self._font_size)
        painter.setFont(label_font)

        label_width = max(42.0, min(side * 0.30, side * 0.42 * segment_angle / 90.0))
        label_height = side * 0.10 if count <= 12 else side * 0.08
        rotate_labels = segment_angle < 45

        for index, prize in enumerate(self._prizes):
            qt_angle = 90 + index * segment_angle + segment_angle / 2 + self._rotation

            if rotate_labels:
                rotate_degrees = -qt_angle
                normalized_degrees = (rotate_degrees + 180) % 360 - 180
                flip_text = normalized_degrees < -90 or normalized_degrees > 90

                painter.save()
                painter.translate(center)
                painter.rotate(rotate_degrees)
                painter.translate(side * 0.30, 0)
                if flip_text:
                    painter.rotate(180)
                label_rect = QRectF(
                    -label_width / 2,
                    -label_height / 2,
                    label_width,
                    label_height,
                )
            else:
                angle_rad = math.radians(qt_angle)
                label_x = center.x() + side * 0.30 * math.cos(angle_rad)
                label_y = center.y() - side * 0.30 * math.sin(angle_rad)
                label_rect = QRectF(
                    label_x - label_width / 2,
                    label_y - label_height / 2,
                    label_width,
                    label_height,
                )

            metrics = painter.fontMetrics()
            elided_name = metrics.elidedText(
                str(prize["name"]),
                Qt.TextElideMode.ElideRight,
                int(label_width),
            )
            painter.setPen(QPen(QColor("#333333")))
            painter.drawText(label_rect, Qt.AlignmentFlag.AlignCenter, elided_name)

            if rotate_labels:
                painter.restore()

        painter.setBrush(QBrush(QColor("#ffffff")))
        painter.setPen(QPen(QColor("#d0d0d0"), 2))
        hub_radius = side * 0.085
        painter.drawEllipse(center, hub_radius, hub_radius)
        painter.setPen(QPen(QColor("#666666")))
        painter.drawText(
            QRectF(center.x() - hub_radius, center.y() - hub_radius, hub_radius * 2, hub_radius * 2),
            Qt.AlignmentFlag.AlignCenter,
            "抽奖",
        )
        painter.setFont(base_font)

        pointer = QPolygonF([
            QPointF(center.x() - 13, wheel_rect.top() - 10),
            QPointF(center.x() + 13, wheel_rect.top() - 10),
            QPointF(center.x(), wheel_rect.top() + 22),
        ])
        painter.setBrush(QBrush(QColor("#e74c3c")))
        painter.setPen(QPen(QColor("#b93224"), 1))
        painter.drawPolygon(pointer)
