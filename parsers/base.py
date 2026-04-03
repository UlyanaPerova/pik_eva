"""
Базовые классы и утилиты для парсеров кладовок.

Модель данных, работа с SQLite (история цен), логгинг, бэкап.
"""
from __future__ import annotations

import logging
import shutil
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml

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


# ─── Модель данных ───────────────────────────────────────

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


# ─── SQLite ──────────────────────────────────────────────

def init_db() -> sqlite3.Connection:
    """Создать/открыть БД и вернуть соединение."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
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
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_item
        ON prices (site, item_id, parsed_at)
    """)
    conn.commit()
    logger.debug("БД инициализирована: %s", DB_PATH)
    return conn


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
                area, price, price_per_meter, original_price, discount_percent, url, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (item.site, item.city, item.complex_name, item.building,
             item.item_id, item.item_number,
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
    Вернуть историю цен (от новых к старым):
    [(price, price_per_meter, original_price, discount_percent, parsed_at), ...]
    """
    rows = conn.execute(
        """SELECT price, price_per_meter, original_price, discount_percent, parsed_at
           FROM prices
           WHERE site = ? AND item_id = ?
           ORDER BY parsed_at DESC""",
        (site, item_id),
    ).fetchall()
    return rows


def get_first_seen_date(conn: sqlite3.Connection, site: str, item_id: str) -> str | None:
    """Вернуть дату первого появления кладовки в БД."""
    row = conn.execute(
        """SELECT parsed_at FROM prices
           WHERE site = ? AND item_id = ?
           ORDER BY parsed_at ASC LIMIT 1""",
        (site, item_id),
    ).fetchone()
    return row[0] if row else None


def get_all_known_ids(conn: sqlite3.Connection, site: str) -> set[str]:
    """Все item_id, которые когда-либо были в БД для данного сайта."""
    rows = conn.execute(
        "SELECT DISTINCT item_id FROM prices WHERE site = ?", (site,)
    ).fetchall()
    return {r[0] for r in rows}


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


# ─── Бэкап ──────────────────────────────────────────────

def backup_db() -> Optional[Path]:
    """Создать бэкап БД перед парсингом. Возвращает путь к бэкапу."""
    if not DB_PATH.exists():
        return None
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"history_{stamp}.db"
    shutil.copy2(DB_PATH, backup_path)
    logger.info("Бэкап БД: %s", backup_path)

    # Удаляем старые бэкапы (оставляем 10 последних)
    backups = sorted(BACKUP_DIR.glob("history_*.db"), reverse=True)
    for old in backups[10:]:
        old.unlink()
        logger.debug("Удалён старый бэкап: %s", old.name)

    return backup_path


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

    # Сайты без цен (дом.рф) — пропускаем проверки цен
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


# ─── Конфиг ──────────────────────────────────────────────

def load_config(config_path: str | Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_config(config: dict, *, require_building: bool = False) -> None:
    """
    Проверить конфиг на ошибки перед запуском парсинга.

    - Каждая запись в links[] должна содержать object_id (int > 0) и complex_name (str).
    - Если require_building=True (конфиг квартир), проверяется наличие поля building.
    - Опциональные поля city/developer — предупреждение, если отсутствуют.
    - Дубликаты по (object_id, building) — предупреждение.

    Raises:
        ValueError: если есть критические ошибки (отсутствуют обязательные поля).
    """
    links = config.get("links")
    if links is None:
        raise ValueError("Конфиг не содержит секцию 'links'")
    if not isinstance(links, list):
        raise ValueError("Секция 'links' должна быть списком")
    if not links:
        logger.warning("Конфиг: секция 'links' пуста — нечего парсить")
        return

    errors: list[str] = []
    seen: set[tuple] = set()

    for idx, entry in enumerate(links):
        prefix = f"links[{idx}]"

        if not isinstance(entry, dict):
            errors.append(f"{prefix}: запись должна быть словарём, получено {type(entry).__name__}")
            continue

        # --- object_id (обязательное) ---
        obj_id = entry.get("object_id")
        if obj_id is None:
            errors.append(f"{prefix}: отсутствует обязательное поле 'object_id'")
        elif not isinstance(obj_id, int):
            errors.append(f"{prefix}: 'object_id' должен быть целым числом, получено {type(obj_id).__name__}: {obj_id!r}")
        elif obj_id <= 0:
            errors.append(f"{prefix}: 'object_id' должен быть > 0, получено {obj_id}")

        # --- complex_name (обязательное) ---
        cname = entry.get("complex_name")
        if cname is None:
            errors.append(f"{prefix}: отсутствует обязательное поле 'complex_name'")
        elif not isinstance(cname, str) or not cname.strip():
            errors.append(f"{prefix}: 'complex_name' должен быть непустой строкой, получено {cname!r}")

        # --- building (обязательное для квартир) ---
        building = entry.get("building")
        if require_building and building is None:
            errors.append(f"{prefix} (object_id={obj_id}): отсутствует обязательное поле 'building'")

        # --- опциональные поля: city, developer ---
        if "city" not in entry:
            logger.warning(
                "Конфиг %s (object_id=%s): отсутствует поле 'city', будет использовано значение по умолчанию",
                prefix, obj_id,
            )
        if "developer" not in entry:
            logger.warning(
                "Конфиг %s (object_id=%s): отсутствует поле 'developer'",
                prefix, obj_id,
            )

        # --- дубликаты ---
        dup_key = (obj_id, entry.get("building", ""))
        if dup_key in seen:
            logger.warning(
                "Конфиг %s: дубликат записи (object_id=%s, building=%r)",
                prefix, obj_id, entry.get("building", ""),
            )
        else:
            seen.add(dup_key)

    if errors:
        msg = "Ошибки валидации конфига:\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Конфиг валиден: %d записей в links", len(links))


# ─── Базовый парсер ──────────────────────────────────────

class BaseParser(ABC):
    def __init__(self, config_path: str | Path):
        self.config = load_config(config_path)
        validate_config(self.config, require_building=False)
        self.site_name: str = self.config["name"]
        self.site_key: str = self.config.get("key", self.site_name.lower())
        self.log = logging.getLogger(f"storehouses.{self.site_key}")

    @abstractmethod
    async def parse_all(self) -> list[StorehouseItem]:
        """Спарсить все кладовки по всем ссылкам из конфига."""
        ...
