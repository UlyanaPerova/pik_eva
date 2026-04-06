"""
Базовые классы и утилиты для парсеров квартир.

Модель данных, работа с SQLite (история цен), логгинг, бэкап.

Этот модуль — тонкая обёртка: реальная логика в parsers/models.py,
parsers/db.py и parsers/config.py. Все публичные имена сохранены
для обратной совместимости.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

# ─── Реэкспорт моделей ─────────────────────────────────
from parsers.models import (  # noqa: F401
    ApartmentItem,
    ROOM_LABELS,
    rooms_label,
)
from parsers.config import load_config, validate_config  # noqa: F401
from parsers.db import (
    init_db as _init_db,
    backup_db as _backup_db,
    get_price_history as _get_price_history,
    get_first_seen_date as _get_first_seen_date,
    get_all_known_ids as _get_all_known_ids,
)

# ─── Пути ───────────────────────────────────────────────
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data" / "apartments"
DB_PATH = DATA_DIR / "apartments_history.db"
BACKUP_DIR = PROJECT_DIR / "backups" / "apartments"
LOG_DIR = PROJECT_DIR / "logs" / "apartments"
OUTPUT_DIR = PROJECT_DIR / "apartments"
BASELINE_DIR = DATA_DIR

# ─── Логгинг ────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("apartments")
logger.setLevel(logging.DEBUG)

_fh = logging.FileHandler(
    LOG_DIR / f"parse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    encoding="utf-8",
)
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))

_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
_ch.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
))

logger.addHandler(_fh)
logger.addHandler(_ch)


# ─── SQLite (обёртки над parsers.db) ────────────────────

_APARTMENTS_CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS apartment_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site TEXT NOT NULL,
        city TEXT NOT NULL DEFAULT '',
        complex_name TEXT NOT NULL,
        building TEXT NOT NULL,
        item_id TEXT NOT NULL,
        rooms INTEGER NOT NULL DEFAULT 0,
        floor INTEGER NOT NULL DEFAULT 0,
        apartment_number TEXT,
        area REAL,
        price REAL,
        price_per_meter REAL,
        original_price REAL,
        discount_percent REAL,
        url TEXT,
        parsed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
"""

_APARTMENTS_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_apt_item
    ON apartment_prices (site, item_id, parsed_at)
"""

def init_db() -> sqlite3.Connection:
    """Создать/открыть БД квартир и вернуть соединение."""
    from parsers.migrations import APARTMENTS_MIGRATIONS
    return _init_db(
        DB_PATH, "apartment_prices", _APARTMENTS_CREATE_SQL, _APARTMENTS_INDEX_SQL,
        versioned_migrations=APARTMENTS_MIGRATIONS,
        log=logger,
    )


def save_items(conn: sqlite3.Connection, items: list[ApartmentItem]) -> int:
    """
    Сохранить спарсенные данные.
    Записывает новую строку если изменилась цена, building или area.
    """
    now = datetime.now().isoformat(timespec="seconds")
    updated = 0
    for item in items:
        row = conn.execute(
            """SELECT price, price_per_meter, building, area FROM apartment_prices
               WHERE site = ? AND item_id = ?
               ORDER BY parsed_at DESC LIMIT 1""",
            (item.site, item.item_id),
        ).fetchone()

        if row:
            price_same = row[0] == item.price and row[1] == item.price_per_meter
            building_same = row[2] == item.building
            area_same = row[3] == item.area
            if price_same and building_same and area_same:
                # Но обновляем object_id если он появился
                if item.object_id:
                    conn.execute(
                        """UPDATE apartment_prices SET object_id = ?
                           WHERE site = ? AND item_id = ? AND object_id IS NULL""",
                        (item.object_id, item.site, item.item_id),
                    )
                continue

        conn.execute(
            """INSERT INTO apartment_prices
               (site, city, complex_name, building, item_id, rooms, floor,
                apartment_number, area, price, price_per_meter,
                original_price, discount_percent, url, living_area,
                object_id, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (item.site, item.city, item.complex_name, item.building,
             item.item_id, item.rooms, item.floor,
             item.apartment_number,
             item.area, item.price, item.price_per_meter,
             item.original_price, item.discount_percent,
             item.url, item.living_area,
             item.object_id, now),
        )
        updated += 1
    conn.commit()
    logger.info("Сохранено %d новых/обновлённых записей из %d", updated, len(items))
    return updated


def get_price_history(
    conn: sqlite3.Connection, site: str, item_id: str
) -> list[tuple]:
    """История цен квартиры (от новых к старым)."""
    return _get_price_history(conn, "apartment_prices", site, item_id)


def get_first_seen_date(conn: sqlite3.Connection, site: str, item_id: str) -> str | None:
    """Дата первого появления квартиры в БД."""
    return _get_first_seen_date(conn, "apartment_prices", site, item_id)


def get_all_known_ids(conn: sqlite3.Connection, site: str) -> set[str]:
    """Все item_id квартир, которые когда-либо были в БД."""
    return _get_all_known_ids(conn, "apartment_prices", site)


def backup_db() -> Optional[Path]:
    """Создать бэкап БД квартир перед парсингом."""
    return _backup_db(DB_PATH, BACKUP_DIR, "apartments", log=logger)


# ─── Baseline ────────────────────────────────────────────

def load_or_create_baseline(items: list[ApartmentItem]) -> set[str]:
    """
    Загрузить baseline (ID квартир из первого парсинга) для каждого сайта.
    Файлы: data/apartments/baseline_pik.json, ...
    """
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    all_baseline: set[str] = set()

    sites_items: dict[str, list[str]] = {}
    for it in items:
        sites_items.setdefault(it.site, []).append(it.item_id)

    for site, item_ids in sites_items.items():
        baseline_path = BASELINE_DIR / f"baseline_{site}.json"
        if baseline_path.exists():
            with open(baseline_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            all_baseline.update(data)
        else:
            with open(baseline_path, "w", encoding="utf-8") as f:
                json.dump(item_ids, f)
            logger.info("Создан baseline квартир %s: %d квартир", site, len(item_ids))
            all_baseline.update(item_ids)

    return all_baseline


# ─── Валидация ───────────────────────────────────────────

def validate_items(items: list[ApartmentItem]) -> list[str]:
    """Проверить спарсенные данные на типичные ошибки."""
    warnings = []
    if not items:
        warnings.append("Парсер вернул 0 квартир!")
        return warnings

    sites_without_prices = {"domrf"}

    for item in items:
        prefix = f"[{item.site}/{item.item_id}]"
        if item.site not in sites_without_prices:
            if item.price <= 0:
                warnings.append(f"{prefix} Цена <= 0: {item.price}")
            if item.price_per_meter <= 0:
                warnings.append(f"{prefix} Цена/м² <= 0: {item.price_per_meter}")
            if item.price < 500_000:
                warnings.append(f"{prefix} Подозрительно низкая цена: {item.price} ₽")
        if item.area <= 0:
            warnings.append(f"{prefix} Площадь <= 0: {item.area}")
        if item.area > 300:
            warnings.append(f"{prefix} Подозрительно большая площадь: {item.area} м²")
        if item.rooms < 0 or item.rooms > 10:
            warnings.append(f"{prefix} Подозрительное кол-во комнат: {item.rooms}")
        if item.discount_percent and item.discount_percent > 50:
            warnings.append(f"{prefix} Подозрительно большая скидка: {item.discount_percent}%")
        if not item.url:
            warnings.append(f"{prefix} Нет URL квартиры")

    logger.info("Валидация: %d квартир, %d предупреждений", len(items), len(warnings))
    for w in warnings:
        logger.warning(w)
    return warnings


# ─── Средние цены ────────────────────────────────────────

def calc_avg_prices(items: list[ApartmentItem]) -> dict:
    """
    Рассчитать средние цены по типам квартир.

    Возвращает:
    {
        "by_rooms": {
            0: {"count": 10, "avg_price": 5_000_000, "avg_ppm": 120_000, ...},
            ...
        },
        "by_complex_rooms": {
            ("Сибирово", 0): {...},
            ...
        },
        "total": {"count": 100, "avg_price": 8_000_000, ...},
    }
    """
    by_rooms: dict[int, list[ApartmentItem]] = defaultdict(list)
    by_complex_rooms: dict[tuple, list[ApartmentItem]] = defaultdict(list)

    for item in items:
        by_rooms[item.rooms].append(item)
        by_complex_rooms[(item.complex_name, item.rooms)].append(item)

    def _stats(group: list[ApartmentItem]) -> dict:
        prices = [it.price for it in group if it.price > 0]
        ppms = [it.price_per_meter for it in group if it.price_per_meter > 0]
        areas = [it.area for it in group if it.area > 0]
        return {
            "count": len(group),
            "avg_price": round(sum(prices) / len(prices)) if prices else 0,
            "avg_ppm": round(sum(ppms) / len(ppms)) if ppms else 0,
            "avg_area": round(sum(areas) / len(areas), 1) if areas else 0,
            "min_price": min(prices) if prices else 0,
            "max_price": max(prices) if prices else 0,
            "min_area": round(min(areas), 1) if areas else 0,
            "max_area": round(max(areas), 1) if areas else 0,
        }

    result = {
        "by_rooms": {r: _stats(group) for r, group in sorted(by_rooms.items())},
        "by_complex_rooms": {
            k: _stats(group) for k, group in sorted(by_complex_rooms.items())
        },
        "total": _stats(items),
    }
    return result


# ─── Базовый парсер ──────────────────────────────────────

class BaseApartmentParser(ABC):
    def __init__(self, config_path: str | Path):
        self.config = load_config(config_path)
        validate_config(self.config, require_building=True, links_key="apartment_links", log=logger)
        self.site_name: str = self.config["name"]
        self.site_key: str = self.config.get("key", self.site_name.lower())
        self.log = logging.getLogger(f"apartments.{self.site_key}")

    @abstractmethod
    async def parse_all(self) -> list[ApartmentItem]:
        """Спарсить все квартиры по всем ссылкам из конфига."""
        ...
