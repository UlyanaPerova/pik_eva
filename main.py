"""
Оркестратор: запуск парсеров → сохранение в SQLite → экспорт xlsx.

Использование:
    python3.13 main.py           # парсить все сайты
    python3.13 main.py pik       # парсить только ПИК
"""
from __future__ import annotations

import asyncio
import sys

from parsers.base import (
    init_db, save_items, backup_db, validate_items, logger,
)
from parsers.pik import PikParser
from exporter import export_xlsx


PARSERS = {
    "pik": PikParser,
    # "akbarsdom": AkBarsDomParser,  # TODO
    # "smu88": Smu88Parser,          # TODO
    # "glorax": GloraxParser,        # TODO
    # "unistroy": UnistroyParser,    # TODO
}


async def run(sites: list[str] | None = None):
    """Запустить парсинг и экспорт."""
    logger.info("=" * 60)
    logger.info("Запуск парсера кладовок")
    logger.info("=" * 60)

    # Бэкап БД
    backup_db()

    # Инициализация БД
    conn = init_db()

    # Определяем какие сайты парсить
    if sites:
        to_parse = {k: v for k, v in PARSERS.items() if k in sites}
        unknown = set(sites) - set(PARSERS.keys())
        if unknown:
            logger.warning("Неизвестные сайты: %s", unknown)
    else:
        to_parse = PARSERS

    # Парсим
    all_items = []
    for site_key, parser_cls in to_parse.items():
        logger.info("─" * 40)
        parser = parser_cls()
        try:
            items = await parser.parse_all()
            warnings = validate_items(items)
            if warnings:
                for w in warnings:
                    logger.warning(w)
            save_items(conn, items)
            all_items.extend(items)
        except Exception as exc:
            logger.error("Ошибка при парсинге %s: %s", site_key, exc)

    # Экспорт xlsx
    if all_items:
        logger.info("─" * 40)
        output = export_xlsx(all_items, conn)
        logger.info("Готово! Файл: %s", output)
    else:
        logger.warning("Нет данных для экспорта")

    conn.close()
    logger.info("=" * 60)
    logger.info("Завершено. Всего кладовок: %d", len(all_items))
    logger.info("=" * 60)


def main():
    sites = sys.argv[1:] if len(sys.argv) > 1 else None
    asyncio.run(run(sites))


if __name__ == "__main__":
    main()
