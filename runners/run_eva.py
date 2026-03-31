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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from eva_calculator import export_eva_xlsx

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


def main():
    parser = argparse.ArgumentParser(description="Генерация расчёта ЕВА")
    parser.add_argument(
        "--sites", nargs="*", default=None,
        help="Фильтр по сайтам (для теста): pik, domrf, akbarsdom, glorax, smu88, unistroy",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Путь для выходного файла (по умолчанию: расчет_ева.xlsx)",
    )
    args = parser.parse_args()

    # Проверяем БД
    if not APT_DB.exists():
        logger.error("БД квартир не найдена: %s", APT_DB)
        return 1
    if not STORE_DB.exists():
        logger.error("БД кладовок не найдена: %s", STORE_DB)
        return 1

    logger.info("=" * 50)
    logger.info("Генерация расчёта ЕВА")
    logger.info("=" * 50)

    # Миграция: добавить living_area, если нет
    conn_apt = sqlite3.connect(str(APT_DB))
    try:
        conn_apt.execute("ALTER TABLE apartment_prices ADD COLUMN living_area REAL")
        conn_apt.commit()
        logger.info("Добавлен столбец living_area в apartment_prices")
    except sqlite3.OperationalError:
        pass

    conn_store = sqlite3.connect(str(STORE_DB))

    output_path = Path(args.output) if args.output else None

    try:
        result = export_eva_xlsx(
            conn_apt=conn_apt,
            conn_store=conn_store,
            filter_sites=args.sites,
            output_path=output_path,
        )
        logger.info("Готово! Файл: %s", result)
        return 0
    except Exception:
        logger.exception("Ошибка генерации")
        return 1
    finally:
        conn_apt.close()
        conn_store.close()


if __name__ == "__main__":
    sys.exit(main())
