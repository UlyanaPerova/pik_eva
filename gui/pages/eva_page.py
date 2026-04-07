"""
PIK EVA GUI — EVA Page (Генерация расчёта ЕВА).
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from nicegui import ui

from gui.runner import DEVELOPERS, RUNNER_SCRIPTS, TaskRunner

PROJECT_DIR = Path(__file__).resolve().parent.parent.parent


def _count_records(db_path: Path, table: str) -> int | None:
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return None


def _get_last_run(db_path: Path, site: str) -> str | None:
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT started_at FROM parse_runs WHERE site = ? ORDER BY started_at DESC LIMIT 1",
            (site,),
        )
        row = cur.fetchone()
        conn.close()
        return row["started_at"] if row else None
    except Exception:
        return None


def _format_dt(dt_str: str | None) -> str:
    if not dt_str:
        return "--"
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%d.%m %H:%M")
    except (ValueError, TypeError):
        return "--"


def eva_page(runner: TaskRunner):
    """Build the EVA page UI."""
    store_db = PROJECT_DIR / "data" / "history.db"
    apt_db = PROJECT_DIR / "data" / "apartments" / "apartments_history.db"

    with ui.column().classes('w-full gap-6 animate-in'):
        with ui.card().classes('glass-card w-full max-w-2xl mx-auto p-6'):
            ui.label('Генерация расчёта ЕВА').classes('text-heading')
            ui.label('Собрать расчет_ева.xlsx из существующих баз данных').style(
                'color: var(--text-secondary); margin-top: 4px; margin-bottom: 16px;'
            )

            # DB status
            apt_count = _count_records(apt_db, "apartment_prices")
            store_count = _count_records(store_db, "prices")

            for label, count in [("БД квартир", apt_count), ("БД кладовок", store_count)]:
                with ui.row().classes('gap-2 items-center'):
                    if count is not None:
                        ui.icon('check_circle').style('color: var(--success); font-size: 18px;')
                        ui.label(f'{label}: {count:,} записей').style(
                            'color: var(--text-primary); font-size: 14px;'
                        )
                    else:
                        ui.icon('cancel').style('color: var(--error); font-size: 18px;')
                        ui.label(f'{label}: не найдена').style(
                            'color: var(--text-muted); font-size: 14px;'
                        )

            ui.separator().classes('my-4')

            # Parse dates table
            all_sites = [k for k, _ in DEVELOPERS] + ["domrf"]
            site_labels = dict(DEVELOPERS + [("domrf", "ДОМ.РФ")])

            ui.label('Последний парсинг').style(
                'font-size: 14px; font-weight: 600; color: var(--text-secondary); margin-bottom: 8px;'
            )
            columns = [
                {'name': 'dev', 'label': 'Застройщик', 'field': 'dev', 'align': 'left'},
                {'name': 'store', 'label': 'Кладовки', 'field': 'store', 'align': 'center'},
                {'name': 'apt', 'label': 'Квартиры', 'field': 'apt', 'align': 'center'},
            ]
            rows = []
            for site in all_sites:
                store_dt = _get_last_run(store_db, site)
                apt_dt = _get_last_run(apt_db, site)
                rows.append({
                    'dev': site_labels.get(site, site),
                    'store': _format_dt(store_dt),
                    'apt': _format_dt(apt_dt),
                })

            ui.table(columns=columns, rows=rows, row_key='dev') \
                .classes('w-full eva-table') \
                .props('dense flat')

            ui.separator().classes('my-4')

            result_container = ui.column().classes('w-full gap-2')

            async def run_eva():
                btn_run.disable()
                tasks = [("eva", "eva", "runners/run_eva.py")]
                await runner.run_tasks(tasks, on_complete=on_done)

            async def on_done():
                btn_run.enable()
                eva_file = PROJECT_DIR / "расчет_ева.xlsx"
                if eva_file.exists():
                    with result_container:
                        result_container.clear()
                        with ui.row().classes('gap-2 items-center'):
                            ui.icon('check_circle').style('color: var(--success);')
                            ui.label('Файл сгенерирован!').style(
                                'color: var(--success); font-weight: 600;'
                            )
                        ui.button('Открыть файл', on_click=lambda: _open_file(eva_file)) \
                            .props('flat no-caps icon=folder_open') \
                            .style('color: var(--primary);')

            btn_run = ui.button('Сгенерировать расчет_ева.xlsx', on_click=run_eva) \
                .classes('w-full mt-4') \
                .props('no-caps size=lg icon=calculate') \
                .style('background: var(--primary); color: white; border-radius: var(--radius-sm);')


def _open_file(path: Path):
    """Open file in system default application."""
    if sys.platform == "darwin":
        subprocess.Popen(["open", str(path)])
    elif sys.platform == "win32":
        os.startfile(str(path))
    else:
        subprocess.Popen(["xdg-open", str(path)])
