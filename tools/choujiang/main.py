"""抽奖转盘程序入口。"""
from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication

from main_window import MainWindow


def main() -> None:
    """启动应用。"""
    app = QApplication(sys.argv)
    app.setApplicationName("抽奖转盘")
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
