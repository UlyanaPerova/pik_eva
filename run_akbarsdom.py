#!/usr/bin/env python3
"""
Запуск парсера Ак Бар Дом + экспорт в xlsx.

Использование:
    python run_akbarsdom.py
"""
from __future__ import annotations

import asyncio
import sys

from parsers.base import (
    init_db, save_items, backup_db, validate_items,
    get_all_known_ids, logger,
)
from parsers.akbarsdom import AkBarsDomParser
from exporter import export_xlsx


async def main() -> int:
    logger.info("=" * 50)
    logger.info("Запуск парсера Ак Бар Дом")
    logger.info("=" * 50)

    backup_db()
    conn = init_db()

    try:
        parser = AkBarsDomParser()
        items = await parser.parse_all()

        if not items:
            logger.error("Парсер не вернул ни одной кладовки!")
            return 1

        warnings = validate_items(items)
        if warnings:
            logger.warning("Обнаружено %d предупреждений валидации", len(warnings))

        previously_known = get_all_known_ids(conn, "akbarsdom")

        updated = save_items(conn, items)
        logger.info("Обновлено записей в БД: %d", updated)

        output_path = export_xlsx(items, conn, previously_known=previously_known)
        logger.info("Файл готов: %s", output_path)

        complexes = set(it.complex_name for it in items)
        logger.info("Статистика:")
        logger.info("  Всего кладовок: %d", len(items))
        logger.info("  ЖК: %s", ", ".join(sorted(complexes)))
        for cname in sorted(complexes):
            cnt = sum(1 for it in items if it.complex_name == cname)
            logger.info("    %s: %d шт.", cname, cnt)

        return 0

    except Exception as exc:
        logger.exception("Критическая ошибка: %s", exc)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
