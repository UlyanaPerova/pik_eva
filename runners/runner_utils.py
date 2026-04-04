"""
Общие утилиты для runners — устранение boilerplate.

Два основных хелпера:
  - run_storehouse_parser()  — для кладовок
  - run_apartment_parser()   — для квартир

Каждый: backup → init_db → parse → validate → save → export → stats → RunResult.
"""
from __future__ import annotations

import logging
import time
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsers.base import BaseParser, StorehouseItem
    from parsers.apartments_base import BaseApartmentParser, ApartmentItem

from runners.run_result import RunResult

PROJECT_DIR = Path(__file__).resolve().parent.parent


def _log_storehouse_stats(
    items: list[StorehouseItem],
    logger: logging.Logger,
) -> None:
    """Вывод статистики по кладовкам."""
    jk_counts = Counter(it.complex_name for it in items)
    logger.info("Статистика:")
    logger.info("  Всего кладовок: %d", len(items))
    logger.info("  ЖК: %s", ", ".join(sorted(jk_counts.keys())))
    for jk in sorted(jk_counts.keys()):
        logger.info("    %s: %d шт.", jk, jk_counts[jk])


def _log_apartment_stats(
    items: list[ApartmentItem],
    logger: logging.Logger,
) -> None:
    """Вывод статистики по квартирам."""
    from parsers.apartments_base import calc_avg_prices, rooms_label

    stats = calc_avg_prices(items)
    logger.info("Статистика:")
    logger.info("  Всего квартир: %d", len(items))
    for r, data in stats["by_rooms"].items():
        if data["avg_price"] > 0:
            logger.info(
                "    %s: %d шт., ср. цена: %s ₽, ср. цена/м²: %s ₽",
                rooms_label(r), data["count"],
                f"{data['avg_price']:,.0f}", f"{data['avg_ppm']:,.0f}",
            )
        else:
            logger.info("    %s: %d шт.", rooms_label(r), data["count"])


async def run_storehouse_parser(
    parser: BaseParser,
    site_key: str,
    site_label: str,
    *,
    export_filename: str | None = None,
) -> RunResult:
    """Запуск парсера кладовок: backup → parse → validate → save → export.

    Args:
        parser: экземпляр парсера (уже создан с конфигом)
        site_key: ключ сайта ('pik', 'glorax', ...)
        site_label: человекочитаемое имя для логов ('ПИК', 'GloraX', ...)
        export_filename: имя xlsx-файла (None = автоматическое)

    Returns:
        RunResult с полной информацией о запуске.
    """
    from parsers.base import (
        init_db, save_items, backup_db, validate_items,
        get_all_known_ids, logger,
    )
    from exporter import export_xlsx

    logger.info("=" * 50)
    logger.info("Запуск парсера %s", site_label)
    logger.info("=" * 50)

    t0 = time.monotonic()
    result = RunResult(success=False, site=site_key)

    backup_db()
    conn = init_db()

    try:
        items = await parser.parse_all()

        if not items:
            result.errors.append("Парсер не вернул ни одной кладовки")
            logger.error("Парсер не вернул ни одной кладовки!")
            return result

        result.items_count = len(items)

        warnings = validate_items(items)
        result.warnings = warnings
        if warnings:
            logger.warning("Обнаружено %d предупреждений валидации", len(warnings))

        previously_known = get_all_known_ids(conn, site_key)

        updated = save_items(conn, items)
        result.items_saved = updated
        logger.info("Обновлено записей в БД: %d", updated)

        if export_filename:
            output_path = PROJECT_DIR / "output" / export_filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            export_xlsx(items, conn, filename=str(output_path))
        else:
            output_path = export_xlsx(items, conn, previously_known=previously_known)

        result.output_path = str(output_path)
        logger.info("Файл готов: %s", output_path)

        _log_storehouse_stats(items, logger)

        result.success = True
        return result

    except Exception as exc:
        logger.exception("Критическая ошибка: %s", exc)
        result.errors.append(str(exc))
        return result
    finally:
        conn.close()
        result.duration_sec = time.monotonic() - t0


async def run_apartment_parser(
    parser: BaseApartmentParser,
    site_key: str,
    site_label: str,
    *,
    add_kvartirografia: bool = True,
) -> RunResult:
    """Запуск парсера квартир: backup → parse → validate → save → export → kvartirografia.

    Args:
        parser: экземпляр парсера квартир
        site_key: ключ сайта ('pik', 'glorax', ...)
        site_label: человекочитаемое имя для логов
        add_kvartirografia: добавлять листы квартирографии (True по умолчанию)

    Returns:
        RunResult с полной информацией о запуске.
    """
    from parsers.apartments_base import (
        init_db, save_items, backup_db, validate_items,
        get_all_known_ids, logger,
    )
    from exporter_apartments import export_apartments_xlsx

    logger.info("=" * 50)
    logger.info("Запуск парсера квартир %s", site_label)
    logger.info("=" * 50)

    t0 = time.monotonic()
    result = RunResult(success=False, site=site_key)

    backup_db()
    conn = init_db()

    try:
        items = await parser.parse_all()

        if not items:
            result.errors.append("Парсер не вернул ни одной квартиры")
            logger.error("Парсер не вернул ни одной квартиры!")
            return result

        result.items_count = len(items)

        warnings = validate_items(items)
        result.warnings = warnings
        if warnings:
            logger.warning("Обнаружено %d предупреждений валидации", len(warnings))

        previously_known = get_all_known_ids(conn, site_key)

        updated = save_items(conn, items)
        result.items_saved = updated
        logger.info("Обновлено записей в БД: %d", updated)

        output_path = export_apartments_xlsx(items, conn, previously_known=previously_known)

        if add_kvartirografia:
            from kvartirografia import add_kvartirografia_sheets
            from openpyxl import load_workbook

            wb = load_workbook(str(output_path))
            add_kvartirografia_sheets(wb, items)
            wb.save(str(output_path))

        result.output_path = str(output_path)
        logger.info("Файл готов: %s", output_path)

        _log_apartment_stats(items, logger)

        result.success = True
        return result

    except Exception as exc:
        logger.exception("Критическая ошибка: %s", exc)
        result.errors.append(str(exc))
        return result
    finally:
        conn.close()
        result.duration_sec = time.monotonic() - t0
