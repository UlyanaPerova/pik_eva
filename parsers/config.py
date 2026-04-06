"""
Общие функции работы с YAML-конфигами парсеров.

Единый load_config/validate_config вместо дублирования в base.py и apartments_base.py.
"""
from __future__ import annotations

import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str | Path) -> dict:
    """Загрузить YAML-конфиг парсера."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_config(
    config: dict,
    *,
    require_building: bool = False,
    links_key: str = "links",
    log: logging.Logger | None = None,
) -> None:
    """
    Проверить конфиг на ошибки перед запуском парсинга.

    - Каждая запись в links[] должна содержать object_id (int > 0) и complex_name (str).
    - Если require_building=True (конфиг квартир), проверяется наличие поля building.
    - Опциональные поля city/developer — предупреждение, если отсутствуют.
    - Дубликаты по (object_id, building) — предупреждение.

    Raises:
        ValueError: если есть критические ошибки (отсутствуют обязательные поля).
    """
    _log = log or logger

    links = config.get(links_key)
    if links is None:
        raise ValueError("Конфиг не содержит секцию 'links'")
    if not isinstance(links, list):
        raise ValueError("Секция 'links' должна быть списком")
    if not links:
        _log.warning("Конфиг: секция 'links' пуста — нечего парсить")
        return

    errors: list[str] = []
    seen: set[tuple] = set()

    for idx, entry in enumerate(links):
        prefix = f"links[{idx}]"

        if not isinstance(entry, dict):
            errors.append(f"{prefix}: запись должна быть словарём, получено {type(entry).__name__}")
            continue

        # --- object_id (обязательное для ДОМ.РФ, необязательное при наличии url) ---
        obj_id = entry.get("object_id")
        has_url = bool(entry.get("url"))
        if obj_id is None and not has_url:
            errors.append(f"{prefix}: отсутствует 'object_id' или 'url'")
        elif obj_id is not None and not isinstance(obj_id, int):
            errors.append(f"{prefix}: 'object_id' должен быть целым числом, получено {type(obj_id).__name__}: {obj_id!r}")
        elif obj_id is not None and obj_id <= 0:
            errors.append(f"{prefix}: 'object_id' должен быть > 0, получено {obj_id}")

        # --- complex_name (обязательное) ---
        cname = entry.get("complex_name")
        if cname is None:
            errors.append(f"{prefix}: отсутствует обязательное поле 'complex_name'")
        elif not isinstance(cname, str) or not cname.strip():
            errors.append(f"{prefix}: 'complex_name' должен быть непустой строкой, получено {cname!r}")

        # --- building (обязательное для квартир, только для ДОМ.РФ-конфигов) ---
        building = entry.get("building")
        if require_building and building is None and not has_url:
            errors.append(f"{prefix} (object_id={obj_id}): отсутствует обязательное поле 'building'")

        # --- опциональные поля: city, developer (только для ДОМ.РФ-конфигов) ---
        if not has_url:
            if "city" not in entry:
                _log.warning(
                    "Конфиг %s (object_id=%s): отсутствует поле 'city'",
                    prefix, obj_id,
                )
            if "developer" not in entry:
                _log.warning(
                    "Конфиг %s (object_id=%s): отсутствует поле 'developer'",
                    prefix, obj_id,
                )

        # --- дубликаты ---
        dup_key = (obj_id, entry.get("building", ""))
        if dup_key in seen:
            _log.warning(
                "Конфиг %s: дубликат записи (object_id=%s, building=%r)",
                prefix, obj_id, entry.get("building", ""),
            )
        else:
            seen.add(dup_key)

    if errors:
        msg = "Ошибки валидации конфига:\n" + "\n".join(f"  - {e}" for e in errors)
        _log.error(msg)
        raise ValueError(msg)

    _log.info("Конфиг валиден: %d записей в links", len(links))
