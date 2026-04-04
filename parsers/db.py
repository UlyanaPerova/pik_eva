"""
Общие функции работы с SQLite для парсеров.

Параметризованные функции вместо дублирования в base.py и apartments_base.py.
Каждая функция принимает параметры БД (путь, имя таблицы) явно.
"""
from __future__ import annotations

import logging
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Инициализация ──────────────────────────────────────

def init_db(
    db_path: Path,
    table_name: str,
    create_sql: str,
    index_sql: str,
    *,
    migrations: list[str] | None = None,
    versioned_migrations: list[tuple[int, str, str]] | None = None,
    log: logging.Logger | None = None,
) -> sqlite3.Connection:
    """Создать/открыть БД, выполнить CREATE TABLE + INDEX + миграции.

    Args:
        migrations: legacy — список SQL-строк (для обратной совместимости)
        versioned_migrations: новые — список (version, description, sql)
    """
    _log = log or logger
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(create_sql)
    conn.execute(index_sql)

    # Legacy миграции (обратная совместимость)
    for migration in (migrations or []):
        try:
            conn.execute(migration)
        except sqlite3.OperationalError:
            pass

    # Версионированные миграции
    if versioned_migrations:
        from parsers.migrations import apply_migrations
        apply_migrations(conn, versioned_migrations, log=_log)

    conn.commit()
    _log.debug("БД инициализирована: %s", db_path)
    return conn


# ─── Бэкап ──────────────────────────────────────────────

def backup_db(
    db_path: Path,
    backup_dir: Path,
    prefix: str,
    *,
    keep: int = 10,
    log: logging.Logger | None = None,
) -> Optional[Path]:
    """Создать бэкап БД. Возвращает путь к бэкапу.

    Args:
        db_path: путь к БД
        backup_dir: папка для бэкапов
        prefix: префикс имени файла (e.g. 'history', 'apartments')
        keep: сколько бэкапов оставлять
    """
    _log = log or logger
    if not db_path.exists():
        return None
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{prefix}_{stamp}.db"
    shutil.copy2(db_path, backup_path)
    _log.info("Бэкап БД: %s", backup_path)

    # Удаляем старые бэкапы
    backups = sorted(backup_dir.glob(f"{prefix}_*.db"), reverse=True)
    for old in backups[keep:]:
        old.unlink()
        _log.debug("Удалён старый бэкап: %s", old.name)

    return backup_path


# ─── Запросы ────────────────────────────────────────────

def get_price_history(
    conn: sqlite3.Connection,
    table: str,
    site: str,
    item_id: str,
) -> list[tuple]:
    """История цен (от новых к старым)."""
    rows = conn.execute(
        f"""SELECT price, price_per_meter, original_price, discount_percent, parsed_at
           FROM {table}
           WHERE site = ? AND item_id = ?
           ORDER BY parsed_at DESC""",
        (site, item_id),
    ).fetchall()
    return rows


def get_first_seen_date(
    conn: sqlite3.Connection,
    table: str,
    site: str,
    item_id: str,
) -> str | None:
    """Дата первого появления item в БД."""
    row = conn.execute(
        f"""SELECT parsed_at FROM {table}
           WHERE site = ? AND item_id = ?
           ORDER BY parsed_at ASC LIMIT 1""",
        (site, item_id),
    ).fetchone()
    return row[0] if row else None


def get_all_known_ids(
    conn: sqlite3.Connection,
    table: str,
    site: str,
) -> set[str]:
    """Все item_id, которые когда-либо были в БД для данного сайта."""
    rows = conn.execute(
        f"SELECT DISTINCT item_id FROM {table} WHERE site = ?", (site,)
    ).fetchall()
    return {r[0] for r in rows}
