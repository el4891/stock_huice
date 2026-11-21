"""配置读写与校验。"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

CONFIG_PATH = Path(__file__).with_name("settings.json")
MIN_COUNT = 1
MAX_COUNT = 20
DEFAULT_COUNT = 4
MIN_FONT_SIZE = 10
MAX_FONT_SIZE = 30
DEFAULT_FONT_SIZE = 12
DEFAULT_SETTINGS_COLLAPSED = False


def default_prizes(count: int = DEFAULT_COUNT) -> list[dict[str, Any]]:
    """生成默认奖项。"""
    count = max(MIN_COUNT, min(MAX_COUNT, count))
    return [{"name": f"奖项{index}", "weight": 1.0} for index in range(1, count + 1)]


def _to_count(value: Any) -> int:
    """把配置中的数量转换成允许范围内的整数。"""
    try:
        count = int(value)
    except (TypeError, ValueError):
        return DEFAULT_COUNT
    return max(MIN_COUNT, min(MAX_COUNT, count))


def _to_font_size(value: Any) -> int:
    """把配置中的字号转换成允许范围内的整数。"""
    try:
        font_size = int(value)
    except (TypeError, ValueError):
        return DEFAULT_FONT_SIZE
    return max(MIN_FONT_SIZE, min(MAX_FONT_SIZE, font_size))


def _to_bool(value: Any) -> bool:
    """把配置中的收起状态转换成布尔值。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return DEFAULT_SETTINGS_COLLAPSED


def _to_weight(value: Any) -> float:
    """把配置中的权重转换成非负有限数字。"""
    try:
        weight = float(value)
    except (TypeError, ValueError):
        return 1.0
    if not math.isfinite(weight) or weight < 0:
        return 1.0
    return weight


def normalize_prizes(prizes: Any) -> list[dict[str, Any]]:
    """校验并标准化奖项列表。"""
    if not isinstance(prizes, list) or not prizes:
        return default_prizes()

    result: list[dict[str, Any]] = []
    for index, raw in enumerate(prizes[:MAX_COUNT], start=1):
        if not isinstance(raw, dict):
            result.append({"name": f"奖项{index}", "weight": 1.0})
            continue

        name = raw.get("name", "")
        if not isinstance(name, str) or not name.strip():
            name = f"奖项{index}"

        result.append({
            "name": name.strip(),
            "weight": _to_weight(raw.get("weight", 1.0)),
        })

    if all(item["weight"] <= 0 for item in result):
        for item in result:
            item["weight"] = 1.0

    return result


def _resize_prizes(prizes: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    """按数量调整奖项列表。"""
    count = _to_count(count)
    if len(prizes) < count:
        for index in range(len(prizes) + 1, count + 1):
            prizes.append({"name": f"奖项{index}", "weight": 1.0})
    return prizes[:count]


def load_config() -> dict[str, Any]:
    """读取配置；文件不存在或损坏时返回默认配置。"""
    prizes = default_prizes()
    font_size = DEFAULT_FONT_SIZE
    settings_collapsed = DEFAULT_SETTINGS_COLLAPSED

    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = None

        if isinstance(data, dict):
            prizes = normalize_prizes(data.get("prizes"))
            prizes = _resize_prizes(prizes, _to_count(data.get("count", DEFAULT_COUNT)))
            font_size = _to_font_size(data.get("font_size", DEFAULT_FONT_SIZE))
            settings_collapsed = _to_bool(
                data.get("settings_collapsed", DEFAULT_SETTINGS_COLLAPSED)
            )

    return {
        "prizes": prizes,
        "font_size": font_size,
        "settings_collapsed": settings_collapsed,
    }


def save_config(
    prizes: list[dict[str, Any]],
    font_size: int,
    settings_collapsed: bool = DEFAULT_SETTINGS_COLLAPSED,
) -> None:
    """保存配置；先写临时文件再替换，避免写入中断损坏原文件。"""
    normalized = normalize_prizes(prizes)
    payload = {
        "version": 1,
        "count": len(normalized),
        "font_size": _to_font_size(font_size),
        "settings_collapsed": _to_bool(settings_collapsed),
        "prizes": normalized,
    }
    temp_path = CONFIG_PATH.with_name(CONFIG_PATH.name + ".tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(CONFIG_PATH)


def calculate_percentages(prizes: list[dict[str, Any]]) -> list[float]:
    """按权重计算百分比。"""
    if not prizes:
        return []

    total = sum(float(item.get("weight", 0)) for item in prizes)
    if total <= 0:
        return [100.0 / len(prizes)] * len(prizes)

    return [float(item.get("weight", 0)) / total * 100 for item in prizes]
