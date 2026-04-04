"""
Модели данных: кладовки и квартиры.

Единый источник истины для dataclass-моделей.
Импортируются из base.py и apartments_base.py для обратной совместимости.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ─── Кладовки ───────────────────────────────────────────

@dataclass
class StorehouseItem:
    """Одна кладовка."""
    site: str                              # 'pik'
    city: str                              # 'Казань'
    complex_name: str                      # 'Сибирово'
    building: str                          # 'Корпус 1'
    item_id: str                           # уникальный ID кладовки
    area: float                            # м²
    price: float                           # ₽ (со скидкой, если есть)
    price_per_meter: float                 # ₽/м²
    url: str                               # ссылка на кладовку
    item_number: Optional[str] = None      # номер кладовой (если есть)
    original_price: Optional[float] = None # цена без скидки (если есть)
    discount_percent: Optional[float] = None  # % скидки (если есть)
    developer: Optional[str] = None        # застройщик (для domrf — из конфига)


# ─── Квартиры ───────────────────────────────────────────

ROOM_LABELS = {
    0: "Студия",
    1: "1-комн.",
    2: "2-комн.",
    3: "3-комн.",
    4: "4-комн.",
    5: "5-комн.",
}


def rooms_label(rooms: int) -> str:
    """Человекочитаемое название типа квартиры."""
    return ROOM_LABELS.get(rooms, f"{rooms}-комн.")


@dataclass
class ApartmentItem:
    """Одна квартира."""
    site: str                              # 'pik'
    city: str                              # 'Казань'
    complex_name: str                      # 'Сибирово'
    building: str                          # 'Корпус 1'
    item_id: str                           # уникальный ID квартиры
    rooms: int                             # 0=студия, 1, 2, 3, ...
    floor: int                             # этаж
    area: float                            # м²
    price: float                           # ₽ (со скидкой, если есть)
    price_per_meter: float                 # ₽/м²
    url: str                               # ссылка на квартиру
    apartment_number: Optional[str] = None # номер квартиры (если есть)
    original_price: Optional[float] = None # цена без скидки (если есть)
    discount_percent: Optional[float] = None  # % скидки (если есть)
    developer: Optional[str] = None        # застройщик (если задан в конфиге)
    living_area: Optional[float] = None    # жилая площа��ь (м²)

    @property
    def rooms_label(self) -> str:
        return ROOM_LABELS.get(self.rooms, f"{self.rooms}-комн.")
