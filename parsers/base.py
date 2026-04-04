"""
Базовые классы и утилиты для парсеров кладовок.

Модель данных, работа с SQLite (история цен), логгинг, бэкап.

Этот модуль — тонкая обёртка: реальная логика в parsers/models.py,
parsers/db.py и parsers/config.py. Все публичные имена сохранены
для обратной совместимости.
"""
from __future__ import annotations

import logging
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional

# ─── Реэкспорт моделей ─────────────────────────────────
from parsers.models import StorehouseItem  # noqa: F401
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
DATA_DIR = PROJECT_DIR / "data"
DB_PATH = DATA_DIR / "history.db"
BACKUP_DIR = PROJECT_DIR / "backups"
LOG_DIR = PROJECT_DIR / "logs"

# ─── Логгинг ────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("storehouses")
logger.setLevel(logging.DEBUG)

# Файловый хэндлер — всё в лог
_fh = logging.FileHandler(
    LOG_DIR / f"parse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    encoding="utf-8",
)
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))

# Консольный хэндлер — INFO и выше
_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
_ch.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
))

logger.addHandler(_fh)
logger.addHandler(_ch)


# ─── SQLite (обёртки над parsers.db) ────────────────────

_STOREHOUSES_CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site TEXT NOT NULL,
        city TEXT NOT NULL DEFAULT '',
        complex_name TEXT NOT NULL,
        building TEXT NOT NULL,
        item_id TEXT NOT NULL,
        item_number TEXT,
        area REAL,
        price REAL,
        price_per_meter REAL,
        original_price REAL,
        discount_percent REAL,
        url TEXT,
        parsed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
"""

_STOREHOUSES_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_item
    ON prices (site, item_id, parsed_at)
"""

def init_db() -> sqlite3.Connection:
    """Создать/открыть БД кладовок и вернуть соединение."""
    from parsers.migrations import STOREHOUSES_MIGRATIONS
    return _init_db(
        DB_PATH, "prices", _STOREHOUSES_CREATE_SQL, _STOREHOUSES_INDEX_SQL,
        versioned_migrations=STOREHOUSES_MIGRATIONS,
        log=logger,
    )


def save_items(conn: sqlite3.Connection, items: list[StorehouseItem]) -> int:
    """
    Сохранить спарсенные данные.
    Записывает новую строку только если цена изменилась.
    Возвращает количество новых/обновлённых записей.
    """
    now = datetime.now().isoformat(timespec="seconds")
    updated = 0
    for item in items:
        row = conn.execute(
            """SELECT price, price_per_meter, original_price FROM prices
               WHERE site = ? AND item_id = ?
               ORDER BY parsed_at DESC LIMIT 1""",
            (item.site, item.item_id),
        ).fetchone()

        # Не пишем если цена не изменилась
        if row and row[0] == item.price and row[1] == item.price_per_meter:
            continue

        conn.execute(
            """INSERT INTO prices
               (site, city, complex_name, building, item_id, item_number,
                area, price, price_per_meter, original_price, discount_percent,
                url, developer, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (item.site, item.city, item.complex_name, item.building,
             item.item_id, item.item_number,
             item.area, item.price, item.price_per_meter,
             item.original_price, item.discount_percent,
             item.url, item.developer, now),
        )
        updated += 1
    conn.commit()
    logger.info("Сохранено %d новых/обновлённых записей из %d", updated, len(items))
    return updated


def get_price_history(
    conn: sqlite3.Connection, site: str, item_id: str
) -> list[tuple]:
    """История цен кладовки (от новых к старым)."""
    return _get_price_history(conn, "prices", site, item_id)


def get_first_seen_date(conn: sqlite3.Connection, site: str, item_id: str) -> str | None:
    """Дата первого появления кладовки в БД."""
    return _get_first_seen_date(conn, "prices", site, item_id)


def get_all_known_ids(conn: sqlite3.Connection, site: str) -> set[str]:
    """Все item_id кладовок, которые когда-либо были в БД."""
    return _get_all_known_ids(conn, "prices", site)


def get_latest_items(conn: sqlite3.Connection, site: str) -> list[dict]:
    """Получить последние актуальные данные по каждой кладовке сайта."""
    rows = conn.execute(
        """SELECT p.* FROM prices p
           INNER JOIN (
               SELECT item_id, MAX(parsed_at) as max_date
               FROM prices WHERE site = ?
               GROUP BY item_id
           ) latest ON p.item_id = latest.item_id AND p.parsed_at = latest.max_date
           WHERE p.site = ?
           ORDER BY p.city, p.complex_name, p.building""",
        (site, site),
    ).fetchall()

    columns = [
        "id", "site", "city", "complex_name", "building", "item_id",
        "item_number", "area", "price", "price_per_meter",
        "original_price", "discount_percent", "url", "parsed_at",
    ]
    return [dict(zip(columns, row)) for row in rows]


def backup_db() -> Optional[Path]:
    """Создать бэкап БД кладовок перед парсингом."""
    return _backup_db(DB_PATH, BACKUP_DIR, "history", log=logger)


# ─── Валидация ───────────────────────────────────────────

def validate_items(items: list[StorehouseItem]) -> list[str]:
    """
    Проверить спарсенные данные на типичные ошибки.
    Возвращает список предупреждений.
    """
    warnings = []
    if not items:
        warnings.append("⚠ Парсер вернул 0 кладовок!")
        return warnings

    sites_without_prices = {"domrf"}

    for i, item in enumerate(items):
        prefix = f"[{item.site}/{item.item_id}]"
        if item.site not in sites_without_prices:
            if item.price <= 0:
                warnings.append(f"{prefix} Цена <= 0: {item.price}")
            if item.price_per_meter <= 0:
                warnings.append(f"{prefix} Цена/м² <= 0: {item.price_per_meter}")
            if item.price < 10_000:
                warnings.append(f"{prefix} Подозрительно низкая цена: {item.price} ₽")
        if item.area <= 0:
            warnings.append(f"{prefix} Площадь <= 0: {item.area}")
        if item.area > 100:
            warnings.append(f"{prefix} Подозрительно большая площадь: {item.area} м²")
        if item.discount_percent and item.discount_percent > 50:
            warnings.append(f"{prefix} Подозрительно большая скидка: {item.discount_percent}%")
        if not item.url:
            warnings.append(f"{prefix} Нет URL кладовки")

    logger.info("Валидация: %d кладовок, %d предупреждений", len(items), len(warnings))
    for w in warnings:
        logger.warning(w)
    return warnings


# ─── Базовый парсер ──────────────────────────────────────

class BaseParser(ABC):
    def __init__(self, config_path: str | Path):
        self.config = load_config(config_path)
        validate_config(self.config, require_building=False, log=logger)
        self.site_name: str = self.config["name"]
        self.site_key: str = self.config.get("key", self.site_name.lower())
        self.log = logging.getLogger(f"storehouses.{self.site_key}")

    @abstractmethod
    async def parse_all(self) -> list[StorehouseItem]:
        """Спарсить все кладовки по всем ссылкам из конфига."""
        ...
