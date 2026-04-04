"""
Единая нормализация поля building.

Заменяет разрозненный `split("||")` и `_norm()` в 10+ файлах.

Формат building в проекте:
  - "1" — простой корпус
  - "М1/ПК-1" — именованный корпус
  - "1||подъезд 2" — корпус + подъезд (ДОМ.РФ)
  - "1||секция 3" — корпус + секция (GloraX)
  - "Корпус 1.1||Секции 1-4" — корпус + примечание (ПИК)
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class NormalizedBuilding:
    """Результат нормализации поля building."""

    primary: str          # основная часть (до "||")
    notes: str            # примечание (после "||"), пустая строка если нет
    entrance: int | None  # номер подъезда (если есть)
    section: str | None   # секция (если есть)
    raw: str              # оригинальное значение


def normalize_building(raw: str) -> NormalizedBuilding:
    """Разобрать поле building на составляющие.

    >>> normalize_building("1||подъезд 2")
    NormalizedBuilding(primary='1', notes='подъезд 2', entrance=2, section=None, raw='1||подъезд 2')

    >>> normalize_building("М1/ПК-1")
    NormalizedBuilding(primary='М1/ПК-1', notes='', entrance=None, section=None, raw='М1/ПК-1')
    """
    if "||" in raw:
        parts = raw.split("||", 1)
        primary = parts[0].strip()
        notes = parts[1].strip()
    else:
        primary = raw.strip()
        notes = ""

    entrance: int | None = None
    section: str | None = None

    if notes:
        # Ищем номер подъезда
        m = re.search(r'подъезд\s*(\d+)', notes, re.IGNORECASE)
        if m:
            entrance = int(m.group(1))

        # Ищем секцию
        m = re.search(r'секция\s*(\S+)', notes, re.IGNORECASE)
        if m:
            section = m.group(1)

    return NormalizedBuilding(
        primary=primary,
        notes=notes,
        entrance=entrance,
        section=section,
        raw=raw,
    )


def building_display(raw: str) -> str:
    """Часть building для отображения в xlsx (без ||notes)."""
    if "||" in raw:
        return raw.split("||", 1)[0].strip()
    return raw.strip()


def building_key(raw: str) -> str:
    """Ключ для matching (нормализованный, как _norm в eva_calculator).

    - Убирает 'Корпус ' prefix
    - Убирает пробелы, дефисы, слеши
    - Приводит к lowercase
    - Берёт только primary часть (до ||)
    """
    s = building_display(raw)
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r'^корпус\s*', '', s)
    s = re.sub(r'[\s\-–—_/]', '', s)
    return s
