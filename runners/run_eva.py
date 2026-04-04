#!/usr/bin/env python3
"""
Оркестрант: генерация единого xlsx расчёта ЕВА.

Читает обе БД (квартиры + кладовки), конфиги, и генерирует файл.
Не запускает парсеры — работает с уже имеющимися данными.

Использование:
    python runners/run_eva.py                      # все данные
    python runners/run_eva.py --sites pik           # только ПИК (для теста)
    python runners/run_eva.py --sites pik domrf     # ПИК + ДОМ.РФ
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from eva_calculator import export_eva_xlsx
from runners.run_result import RunResult

# Пути к БД
PROJECT_DIR = Path(__file__).resolve().parent.parent
APT_DB = PROJECT_DIR / "data" / "apartments" / "apartments_history.db"
STORE_DB = PROJECT_DIR / "data" / "history.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_eva")


def main() -> int:
    ap = argparse.ArgumentParser(description="Генерация расчёта ЕВА")
    ap.add_argument(
        "--sites", nargs="*", default=None,
        help="Фильтр по сайтам (для теста): pik, domrf, akbarsdom, glorax, smu88, unistroy",
    )
    ap.add_argument(
        "--output", type=str, default=None,
        help="Путь для выходного файла (по умолчанию: расчет_ева.xlsx)",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Валидация без записи файла — только проверка данных",
    )
    args = ap.parse_args()

    t0 = time.monotonic()
    result = RunResult(success=False, site="eva")

    # Проверяем БД
    if not APT_DB.exists():
        logger.error("БД квартир не найдена: %s", APT_DB)
        result.errors.append(f"БД квартир не найдена: {APT_DB}")
        return result.exit_code
    if not STORE_DB.exists():
        logger.error("БД кладовок не найдена: %s", STORE_DB)
        result.errors.append(f"БД кладовок не найдена: {STORE_DB}")
        return result.exit_code

    logger.info("=" * 50)
    logger.info("Генерация расчёта ЕВА")
    logger.info("=" * 50)

    # Миграции: применить все накопленные миграции к обеим БД
    from parsers.migrations import apply_migrations, APARTMENTS_MIGRATIONS, STOREHOUSES_MIGRATIONS

    conn_apt = sqlite3.connect(str(APT_DB))
    apply_migrations(conn_apt, APARTMENTS_MIGRATIONS, log=logger)

    conn_store = sqlite3.connect(str(STORE_DB))
    apply_migrations(conn_store, STOREHOUSES_MIGRATIONS, log=logger)

    output_path = Path(args.output) if args.output else None

    try:
        if args.dry_run:
            # Dry-run: только агрегация и валидация, без записи файла
            from eva_calculator import _aggregate, _load_apartments, _load_storehouses
            logger.info("DRY-RUN: валидация данных без записи файла")

            apts = _load_apartments(conn_apt, args.sites[0] if args.sites and len(args.sites) == 1 else None)
            stores = _load_storehouses(conn_store, args.sites[0] if args.sites and len(args.sites) == 1 else None)
            logger.info("Загружено: %d квартир, %d кладовок", len(apts), len(stores))

            buildings = _aggregate(conn_apt, conn_store, args.sites)
            logger.info("Агрегировано: %d корпусов", len(buildings))

            empty = sum(1 for b in buildings if b.domrf_apt_count == 0)
            if empty:
                logger.warning("Корпусов без квартир ДОМ.РФ: %d из %d", empty, len(buildings))

            result.success = True
            result.items_count = len(buildings)
            logger.info("DRY-RUN завершён. Данные валидны.")
            return result.exit_code

        xlsx_path = export_eva_xlsx(
            conn_apt=conn_apt,
            conn_store=conn_store,
            filter_sites=args.sites,
            output_path=output_path,
        )
        result.success = True
        result.output_path = str(xlsx_path)
        logger.info("Готово! Файл: %s", xlsx_path)
        return result.exit_code
    except Exception:
        logger.exception("Ошибка генерации")
        result.errors.append("Ошибка генерации EVA xlsx")
        return result.exit_code
    finally:
        conn_apt.close()
        conn_store.close()
        result.duration_sec = time.monotonic() - t0


if __name__ == "__main__":
    sys.exit(main())
