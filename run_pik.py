#!/usr/bin/env python3
"""
Запуск парсера ПИК + экспорт в xlsx.

Использование:
    python run_pik.py
"""
from __future__ import annotations

import asyncio
import sys

from parsers.base import init_db, save_items, backup_db, validate_items, logger
from parsers.pik import PikParser
from exporter import export_xlsx


async def main() -> int:
    logger.info("=" * 50)
    logger.info("Запуск парсера ПИК")
    logger.info("=" * 50)

    # Бэкап БД
    backup_db()

    # Инициализация БД
    conn = init_db()

    try:
        # Парсинг
        parser = PikParser()
        items = await parser.parse_all()

        if not items:
            logger.error("Парсер не вернул ни одной кладовки!")
            return 1

        # Валидация
        warnings = validate_items(items)
        if warnings:
            logger.warning("Обнаружено %d предупреждений валидации", len(warnings))

        # Сохранение в БД
        updated = save_items(conn, items)
        logger.info("Обновлено записей в БД: %d", updated)

        # Экспорт xlsx
        output_path = export_xlsx(items, conn)
        logger.info("Файл готов: %s", output_path)

        # Краткая статистика
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
