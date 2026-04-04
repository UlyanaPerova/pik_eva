"""
Система миграций SQLite для баз данных проекта.

Заменяет разрозненные `try: ALTER TABLE... except: pass`
на версионированные миграции с таблицей schema_version.
"""
from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)


def _ensure_version_table(conn: sqlite3.Connection) -> None:
    """Создать таблицу schema_version, если не существует."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY
        )
    """)
    conn.commit()


def get_version(conn: sqlite3.Connection) -> int:
    """Получить текущую версию схемы БД."""
    _ensure_version_table(conn)
    row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
    return row[0] or 0


def _set_version(conn: sqlite3.Connection, version: int) -> None:
    """Установить версию схемы."""
    conn.execute("INSERT OR REPLACE INTO schema_version (version) VALUES (?)", (version,))
    conn.commit()


def apply_migrations(
    conn: sqlite3.Connection,
    migrations: list[tuple[int, str, str]],
    *,
    log: logging.Logger | None = None,
) -> int:
    """Применить миграции к БД.

    Args:
        conn: соединение с БД
        migrations: список (version, description, sql_statement)
        log: логгер

    Returns:
        Количество применённых миграций.
    """
    _log = log or logger
    current = get_version(conn)
    applied = 0

    for version, description, sql in sorted(migrations):
        if version <= current:
            continue
        try:
            conn.execute(sql)
            _set_version(conn, version)
            applied += 1
            _log.info("Миграция v%d: %s — OK", version, description)
        except sqlite3.OperationalError as e:
            # Столбец уже существует — пропускаем без ошибки
            if "duplicate column" in str(e).lower():
                _set_version(conn, version)
                _log.debug("Миграция v%d: %s — пропущена (уже применена)", version, description)
            else:
                _log.error("Миграция v%d: %s — ОШИБКА: %s", version, description, e)
                raise

    if applied:
        _log.info("Применено %d миграций, текущая версия: %d", applied, get_version(conn))

    return applied


# ─── Миграции для кладовок (prices) ─────────────────────

STOREHOUSES_MIGRATIONS = [
    (1, "Добавить столбец developer", "ALTER TABLE prices ADD COLUMN developer TEXT"),
]


# ─── Миграции для квартир (apartment_prices) ────────────

APARTMENTS_MIGRATIONS = [
    (1, "Добавить столбец living_area", "ALTER TABLE apartment_prices ADD COLUMN living_area REAL"),
    (2, "Добавить столбец developer", "ALTER TABLE apartment_prices ADD COLUMN developer TEXT"),
]


# ─── Таблица parse_runs для отслеживания свежести данных ─

PARSE_RUNS_SQL = """
    CREATE TABLE IF NOT EXISTS parse_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site TEXT NOT NULL,
        started_at DATETIME NOT NULL,
        finished_at DATETIME,
        items_count INTEGER DEFAULT 0,
        items_saved INTEGER DEFAULT 0,
        success INTEGER DEFAULT 0,
        errors TEXT,
        duration_sec REAL DEFAULT 0
    )
"""

PARSE_RUNS_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_parse_runs_site
    ON parse_runs (site, started_at)
"""


def init_parse_runs(conn: sqlite3.Connection) -> None:
    """Создать таблицу parse_runs, если не существует."""
    conn.execute(PARSE_RUNS_SQL)
    conn.execute(PARSE_RUNS_INDEX_SQL)
    conn.commit()


def record_parse_run(
    conn: sqlite3.Connection,
    site: str,
    started_at: str,
    finished_at: str,
    items_count: int,
    items_saved: int,
    success: bool,
    errors: str = "",
    duration_sec: float = 0.0,
) -> None:
    """Записать результат запуска парсера."""
    init_parse_runs(conn)
    conn.execute(
        """INSERT INTO parse_runs
           (site, started_at, finished_at, items_count, items_saved, success, errors, duration_sec)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (site, started_at, finished_at, items_count, items_saved, int(success), errors, duration_sec),
    )
    conn.commit()


def get_last_parse_run(conn: sqlite3.Connection, site: str) -> dict | None:
    """Получить последний запуск парсера для сайта."""
    init_parse_runs(conn)
    row = conn.execute(
        """SELECT site, started_at, finished_at, items_count, items_saved,
                  success, errors, duration_sec
           FROM parse_runs WHERE site = ?
           ORDER BY started_at DESC LIMIT 1""",
        (site,),
    ).fetchone()
    if not row:
        return None
    return {
        "site": row[0],
        "started_at": row[1],
        "finished_at": row[2],
        "items_count": row[3],
        "items_saved": row[4],
        "success": bool(row[5]),
        "errors": row[6],
        "duration_sec": row[7],
    }
