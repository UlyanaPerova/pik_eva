"""
PIK EVA GUI — Developer Card widget.

Glass-карточка застройщика с тумблером, чекбоксами и статусом.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from nicegui import ui

PROJECT_DIR = Path(__file__).resolve().parent.parent.parent


def _get_last_run(db_path: Path, site: str) -> dict | None:
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM parse_runs WHERE site = ? ORDER BY started_at DESC LIMIT 1",
            (site,),
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


def _format_date(dt_str: str | None) -> str:
    if not dt_str:
        return "--"
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%d.%m.%Y %H:%M")
    except (ValueError, TypeError):
        return dt_str[:16] if dt_str else "--"


def _freshness_color(dt_str: str | None) -> str:
    if not dt_str:
        return "var(--error)"
    try:
        dt = datetime.fromisoformat(dt_str)
        days = (datetime.now() - dt).days
        if days < 3:
            return "var(--success)"
        elif days < 7:
            return "var(--warning)"
        return "var(--error)"
    except (ValueError, TypeError):
        return "var(--error)"


class DeveloperCard:
    """A glass card for a single developer with store/apt toggles."""

    def __init__(self, key: str, label: str, is_domrf: bool = False):
        self.key = key
        self.label = label
        self.is_domrf = is_domrf
        self.enabled = True
        self.store_checked = False
        self.apt_checked = False
        self._card: ui.card | None = None
        self._content_container = None
        self._cb_store = None
        self._cb_apt = None

    def build(self) -> ui.card:
        extra = 'w-full' if self.is_domrf else ''
        self._card = ui.card().classes(f'glass-card p-5 {extra}')

        with self._card:
            with ui.row().classes('w-full justify-between items-center mb-3'):
                ui.label(self.label).style(
                    'font-size: 16px; font-weight: 600; color: var(--text-primary);'
                )
                ui.switch(value=True, on_change=self._on_toggle) \
                    .props('dense color=blue')

            self._content_container = ui.column().classes('gap-2 w-full')
            with self._content_container:
                with ui.row().classes('gap-6'):
                    self._cb_store = ui.checkbox('Кладовки', value=self.store_checked,
                                on_change=lambda e: setattr(self, 'store_checked', e.value)) \
                        .style('color: var(--text-secondary);')
                    self._cb_apt = ui.checkbox('Квартиры', value=self.apt_checked,
                                on_change=lambda e: setattr(self, 'apt_checked', e.value)) \
                        .style('color: var(--text-secondary);')

                self._build_status_info()

        return self._card

    def _build_status_info(self):
        store_db = PROJECT_DIR / "data" / "history.db"
        apt_db = PROJECT_DIR / "data" / "apartments" / "apartments_history.db"

        store_run = _get_last_run(store_db, self.key)
        apt_run = _get_last_run(apt_db, self.key)

        store_date = store_run["started_at"] if store_run else None
        apt_date = apt_run["started_at"] if apt_run else None

        with ui.column().classes('gap-1 mt-2'):
            with ui.row().classes('gap-2 items-center'):
                ui.icon('inventory_2').style(
                    f'font-size: 14px; color: {_freshness_color(store_date)};'
                )
                lbl = f'Кладовки: {_format_date(store_date)}'
                if store_run and store_run.get("items_count"):
                    lbl += f' ({store_run["items_count"]} шт)'
                ui.label(lbl).style('font-size: 12px; color: var(--text-muted);')

            with ui.row().classes('gap-2 items-center'):
                ui.icon('apartment').style(
                    f'font-size: 14px; color: {_freshness_color(apt_date)};'
                )
                lbl = f'Квартиры: {_format_date(apt_date)}'
                if apt_run and apt_run.get("items_count"):
                    lbl += f' ({apt_run["items_count"]} шт)'
                ui.label(lbl).style('font-size: 12px; color: var(--text-muted);')

    def _on_toggle(self, e):
        self.enabled = e.value
        if self._content_container:
            if e.value:
                self._content_container.classes(remove='card-disabled')
            else:
                self._content_container.classes(add='card-disabled')

    def set_all_checked(self, checked: bool):
        """Programmatically set both checkboxes."""
        self.store_checked = checked
        self.apt_checked = checked
        if self._cb_store:
            self._cb_store.value = checked
        if self._cb_apt:
            self._cb_apt.value = checked

    def set_running(self, running: bool):
        if self._card:
            if running:
                self._card.classes(add='running')
            else:
                self._card.classes(remove='running')

    def get_tasks(self) -> list[tuple[str, str, str]]:
        from gui.runner import RUNNER_SCRIPTS
        tasks = []
        if not self.enabled:
            return tasks
        if self.store_checked:
            script = RUNNER_SCRIPTS.get(("store", self.key))
            if script:
                tasks.append(("store", self.key, script))
        if self.apt_checked:
            script = RUNNER_SCRIPTS.get(("apt", self.key))
            if script:
                tasks.append(("apt", self.key, script))
        return tasks
