"""
Базовые классы и утилиты для парсеров квартир.

Модель данных, работа с SQLite (история цен), логгинг, бэкап.
Аналогично base.py для кладовок, но со спецификой квартир:
  - rooms (количество комнат: 0=студия, 1, 2, 3, ...)
  - floor (этаж)
  - средние цены по типам квартир
"""
from __future__ import annotations

import json
import logging
import shutil
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml

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


# ─── Модель данных ───────────────────────────────────────

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
    living_area: Optional[float] = None    # жилая площадь (м²)

    @property
    def rooms_label(self) -> str:
        return ROOM_LABELS.get(self.rooms, f"{self.rooms}-комн.")


# ─── SQLite ──────────────────────────────────────────────

def init_db() -> sqlite3.Connection:
    """Создать/открыть БД квартир и вернуть соединение."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
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
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_apt_item
        ON apartment_prices (site, item_id, parsed_at)
    """)
    conn.commit()
    logger.debug("БД квартир инициализирована: %s", DB_PATH)
    return conn


def save_items(conn: sqlite3.Connection, items: list[ApartmentItem]) -> int:
    """
    Сохранить спарсенные данные.
    Записывает новую строку только если цена изменилась.
    """
    now = datetime.now().isoformat(timespec="seconds")
    updated = 0
    for item in items:
        row = conn.execute(
            """SELECT price, price_per_meter FROM apartment_prices
               WHERE site = ? AND item_id = ?
               ORDER BY parsed_at DESC LIMIT 1""",
            (item.site, item.item_id),
        ).fetchone()

        if row and row[0] == item.price and row[1] == item.price_per_meter:
            continue

        conn.execute(
            """INSERT INTO apartment_prices
               (site, city, complex_name, building, item_id, rooms, floor,
                apartment_number, area, price, price_per_meter,
                original_price, discount_percent, url, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (item.site, item.city, item.complex_name, item.building,
             item.item_id, item.rooms, item.floor,
             item.apartment_number,
             item.area, item.price, item.price_per_meter,
             item.original_price, item.discount_percent,
             item.url, now),
        )
        updated += 1
    conn.commit()
    logger.info("Сохранено %d новых/обновлённых записей из %d", updated, len(items))
    return updated


def get_price_history(
    conn: sqlite3.Connection, site: str, item_id: str
) -> list[tuple]:
    """
    Вернуть историю цен квартиры (от новых к старым):
    [(price, price_per_meter, original_price, discount_percent, parsed_at), ...]
    """
    rows = conn.execute(
        """SELECT price, price_per_meter, original_price, discount_percent, parsed_at
           FROM apartment_prices
           WHERE site = ? AND item_id = ?
           ORDER BY parsed_at DESC""",
        (site, item_id),
    ).fetchall()
    return rows


def get_first_seen_date(conn: sqlite3.Connection, site: str, item_id: str) -> str | None:
    """Вернуть дату первого появления квартиры в БД."""
    row = conn.execute(
        """SELECT parsed_at FROM apartment_prices
           WHERE site = ? AND item_id = ?
           ORDER BY parsed_at ASC LIMIT 1""",
        (site, item_id),
    ).fetchone()
    return row[0] if row else None


def get_all_known_ids(conn: sqlite3.Connection, site: str) -> set[str]:
    """Все item_id, которые когда-либо были в БД для данного сайта."""
    rows = conn.execute(
        "SELECT DISTINCT item_id FROM apartment_prices WHERE site = ?", (site,)
    ).fetchall()
    return {r[0] for r in rows}


# ─── Бэкап ──────────────────────────────────────────────

def backup_db() -> Optional[Path]:
    """Создать бэкап БД перед парсингом."""
    if not DB_PATH.exists():
        return None
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"apartments_{stamp}.db"
    shutil.copy2(DB_PATH, backup_path)
    logger.info("Бэкап БД квартир: %s", backup_path)

    backups = sorted(BACKUP_DIR.glob("apartments_*.db"), reverse=True)
    for old in backups[10:]:
        old.unlink()
        logger.debug("Удалён старый бэкап: %s", old.name)

    return backup_path


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

    # Сайты без цен (дом.рф) — пропускаем проверки цен
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
            0: {"count": 10, "avg_price": 5_000_000, "avg_ppm": 120_000,
                "min_price": 3_500_000, "max_price": 7_000_000},
            1: {...},
            ...
        },
        "by_complex_rooms": {
            ("Сибирово", 0): {...},
            ...
        },
        "total": {"count": 100, "avg_price": 8_000_000, "avg_ppm": 110_000,
                  "min_price": 3_000_000, "max_price": 20_000_000},
    }
    """
    from collections import defaultdict

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


# ─── Конфиг ──────────────────────────────────────────────

def load_config(config_path: str | Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ─── Базовый парсер ──────────────────────────────────────

class BaseApartmentParser(ABC):
    def __init__(self, config_path: str | Path):
        self.config = load_config(config_path)
        self.site_name: str = self.config["name"]
        self.site_key: str = self.config.get("key", self.site_name.lower())
        self.log = logging.getLogger(f"apartments.{self.site_key}")

    @abstractmethod
    async def parse_all(self) -> list[ApartmentItem]:
        """Спарсить все квартиры по всем ссылкам из конфига."""
        ...
