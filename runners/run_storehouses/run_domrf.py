#!/usr/bin/env python3
"""Запуск парсера ДОМ.РФ (наш.дом.рф)."""
import asyncio
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from parsers.domrf import DomRfParser
from parsers.base import init_db, save_items, backup_db, validate_items, logger
from exporter import export_xlsx


def _parse_args():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cdp", type=int, default=None, metavar="PORT",
                    help="Подключиться к Chrome через CDP (порт remote-debugging)")
    return ap.parse_args()


async def main():
    args = _parse_args()

    logger.info("=" * 50)
    logger.info("Запуск парсера ДОМ.РФ")
    logger.info("=" * 50)

    backup_db()

    parser = DomRfParser(cdp_port=args.cdp)
    items = await parser.parse_all()

    warnings = validate_items(items)

    conn = init_db()
    updated = save_items(conn, items)
    logger.info("Обновлено записей в БД: %d", updated)

    output_path = PROJECT_DIR / "output" / "storehouses_DomRF.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    export_xlsx(items, conn, filename=str(output_path))
    logger.info("Файл готов: %s", output_path)

    # Статистика
    from collections import Counter
    jk_counts = Counter(it.complex_name for it in items)
    logger.info("Статистика:")
    logger.info("  Всего кладовок: %d", len(items))
    logger.info("  ЖК: %s", ", ".join(sorted(jk_counts.keys())))
    for jk in sorted(jk_counts.keys()):
        logger.info("    %s: %d шт.", jk, jk_counts[jk])

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
