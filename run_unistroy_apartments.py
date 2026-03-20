#!/usr/bin/env python3
"""
Запуск парсера квартир УниСтрой + экспорт в xlsx.

Использование:
    python run_unistroy_apartments.py
"""
from __future__ import annotations

import asyncio
import sys

from parsers.apartments_base import (
    init_db, save_items, backup_db, validate_items,
    get_all_known_ids, calc_avg_prices, rooms_label, logger,
)
from parsers.unistroy_apartments import UnistroyApartmentParser
from exporter_apartments import export_apartments_xlsx


async def main() -> int:
    logger.info("=" * 50)
    logger.info("Запуск парсера квартир УниСтрой")
    logger.info("=" * 50)

    backup_db()
    conn = init_db()

    try:
        parser = UnistroyApartmentParser()
        items = await parser.parse_all()

        if not items:
            logger.error("Парсер не вернул ни одной квартиры!")
            return 1

        warnings = validate_items(items)
        if warnings:
            logger.warning("Обнаружено %d предупреждений валидации", len(warnings))

        previously_known = get_all_known_ids(conn, "unistroy")

        updated = save_items(conn, items)
        logger.info("Обновлено записей в БД: %d", updated)

        output_path = export_apartments_xlsx(items, conn, previously_known=previously_known)
        logger.info("Файл готов: %s", output_path)

        stats = calc_avg_prices(items)
        logger.info("Статистика:")
        logger.info("  Всего квартир: %d", len(items))
        for r, data in stats["by_rooms"].items():
            logger.info("    %s: %d шт., ср. цена: %s ₽",
                        rooms_label(r), data["count"], f"{data['avg_price']:,.0f}")

        return 0

    except Exception as exc:
        logger.exception("Критическая ошибка: %s", exc)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
