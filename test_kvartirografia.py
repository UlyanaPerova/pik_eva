#!/usr/bin/env python3
"""Тест квартирографии на 3 объектах."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from parsers.apartments_base import (
    init_db, save_items, validate_items,
    get_all_known_ids, logger,
)
from parsers.domrf_apartments import DomRfApartmentParser, ObjectInfo
from exporter_apartments import export_apartments_xlsx

from openpyxl import load_workbook
from run_domrf_apartments import (
    _add_kvartirografia_sheet, _add_object_info_sheet,
)


async def main() -> int:
    logger.info("ТЕСТ квартирографии — 3 объекта")

    conn = init_db()

    try:
        parser = DomRfApartmentParser()

        # Берём только первые 3 объекта из конфига
        original_links = parser.config.get("links", [])
        parser.config["links"] = original_links[:3]

        items, object_infos = await parser.parse_all()

        if not items:
            logger.error("Нет квартир!")
            return 1

        logger.info("Получено %d квартир из %d объектов", len(items), len(parser.config["links"]))

        previously_known = get_all_known_ids(conn, "domrf")

        output_path = Path("apartments/test_kvartirografia.xlsx")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        from parsers.apartments_base import OUTPUT_DIR
        result = export_apartments_xlsx(
            items, conn,
            filename="test_kvartirografia.xlsx",
            previously_known=previously_known,
        )

        wb = load_workbook(str(result))
        _add_kvartirografia_sheet(wb, items, object_infos)
        _add_object_info_sheet(wb, object_infos)
        wb.save(str(result))

        logger.info("Тестовый файл: %s", result)
        return 0

    except Exception as exc:
        logger.exception("Ошибка: %s", exc)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
