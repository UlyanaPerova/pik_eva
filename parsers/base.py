from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

import yaml

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DB_PATH = DATA_DIR / "history.db"


@dataclass
class StorehouseItem:
    site: str               # 'pik'
    complex_name: str       # 'Сибирово'
    building: str           # 'Корпус 1'
    item_id: str            # уникальный ID кладовки
    area: float             # м²
    price: float            # ₽
    price_per_meter: float  # ₽/м²


def init_db() -> sqlite3.Connection:
    """Создать/открыть БД и вернуть соединение."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site TEXT NOT NULL,
            complex_name TEXT NOT NULL,
            building TEXT NOT NULL,
            item_id TEXT NOT NULL,
            area REAL,
            price REAL,
            price_per_meter REAL,
            parsed_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_item
        ON prices (site, item_id, parsed_at)
    """)
    conn.commit()
    return conn


def save_items(conn: sqlite3.Connection, items: list[StorehouseItem]) -> None:
    """Сохранить спарсенные данные. Записывает только если цена изменилась."""
    now = datetime.now().isoformat(timespec="seconds")
    for item in items:
        row = conn.execute(
            """SELECT price, price_per_meter FROM prices
               WHERE site = ? AND item_id = ?
               ORDER BY parsed_at DESC LIMIT 1""",
            (item.site, item.item_id),
        ).fetchone()

        if row and row[0] == item.price and row[1] == item.price_per_meter:
            continue  # цена не изменилась

        conn.execute(
            """INSERT INTO prices
               (site, complex_name, building, item_id, area, price, price_per_meter, parsed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (item.site, item.complex_name, item.building,
             item.item_id, item.area, item.price, item.price_per_meter, now),
        )
    conn.commit()


def get_price_history(conn: sqlite3.Connection, site: str, item_id: str) -> list[tuple[float, float, str]]:
    """Вернуть историю цен: [(price, price_per_meter, parsed_at), ...] от новых к старым."""
    rows = conn.execute(
        """SELECT price, price_per_meter, parsed_at FROM prices
           WHERE site = ? AND item_id = ?
           ORDER BY parsed_at DESC""",
        (site, item_id),
    ).fetchall()
    return rows


def load_config(config_path: str | Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class BaseParser(ABC):
    def __init__(self, config_path: str | Path):
        self.config = load_config(config_path)
        self.site_name: str = self.config["name"]
        self.site_key: str = self.config.get("key", self.site_name.lower())

    @abstractmethod
    async def parse_all(self) -> list[StorehouseItem]:
        """Спарсить все кладовки по всем ссылкам из конфига."""
        ...
