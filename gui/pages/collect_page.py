"""
PIK EVA GUI — Collect Page (Первичный сбор).
"""
from __future__ import annotations

import time

from nicegui import ui

from gui.runner import DEVELOPERS, TaskRunner
from gui.widgets.developer_card import DeveloperCard

# Примерное время на один парсер (секунды)
EST_TIME_PER_TASK = 90


def collect_page(runner: TaskRunner):
    """Build the Collect page UI."""
    cards: list[DeveloperCard] = []

    with ui.column().classes('w-full gap-6 animate-in'):
        ui.label('Первичный сбор данных (делается один раз)').classes('text-heading')

        # ── Select all ──
        with ui.card().classes('glass-card w-full p-4'):
            with ui.row().classes('items-center gap-4'):
                ui.checkbox('Выбрать всё', value=False,
                            on_change=lambda e: _select_all(cards, e.value)) \
                    .style('color: var(--text-primary); font-weight: 600;')

        # ── Developer cards ──
        with ui.grid(columns=2).classes('w-full gap-4'):
            for key, label in DEVELOPERS:
                card = DeveloperCard(key, label)
                card.build()
                cards.append(card)

        domrf_card = DeveloperCard("domrf", "ДОМ.РФ", is_domrf=True)
        domrf_card.build()
        cards.append(domrf_card)

        # ── Progress area ──
        progress_card = ui.card().classes('glass-card w-full p-4')
        progress_card.set_visibility(False)
        with progress_card:
            with ui.row().classes('items-center gap-3 mb-2'):
                spinner_el = ui.html('<div class="loading-spinner"></div>')
                progress_text = ui.label('').style(
                    'font-size: 14px; font-weight: 500; color: var(--text-primary);'
                )
            progress_bar = ui.linear_progress(value=0, show_value=False) \
                .classes('w-full').props('rounded')
            with ui.row().classes('w-full justify-between mt-1'):
                task_label = ui.label('').style(
                    'font-size: 12px; color: var(--text-muted);'
                )
                eta_label = ui.label('').style(
                    'font-size: 12px; color: var(--text-muted);'
                )

        btn = ui.button('Запустить сбор',
                        on_click=lambda: _run(runner, cards, btn,
                                              progress_card, progress_bar, progress_text,
                                              task_label, eta_label, spinner_el)) \
            .classes('w-full mt-2') \
            .props('no-caps size=lg icon=play_arrow') \
            .style('background: var(--primary); color: white; border-radius: var(--radius-sm);')


def _select_all(cards: list[DeveloperCard], checked: bool):
    """Toggle all checkboxes on all cards."""
    for card in cards:
        card.set_all_checked(checked)


async def _run(r, cards, btn, progress_card, progress_bar, progress_text,
               task_label, eta_label, spinner_el):
    tasks = []
    for card in cards:
        tasks.extend(card.get_tasks())
    if not tasks:
        ui.notify('Ничего не выбрано — отметьте кладовки/квартиры', type='warning')
        return

    btn.disable()
    progress_card.set_visibility(True)
    total = len(tasks)
    start_time = time.time()
    est_total = total * EST_TIME_PER_TASK

    card_map = {card.key: card for card in cards}

    def _fmt_eta(seconds: int) -> str:
        if seconds < 60:
            return f"~{seconds} сек"
        m = seconds // 60
        s = seconds % 60
        return f"~{m} мин {s} сек"

    progress_text.text = f'Выполняется: 0 / {total}'
    eta_label.text = f'Ожидание: {_fmt_eta(est_total)}'
    progress_bar.value = 0

    completed = [0]

    async def on_status(task_id: str, status: str):
        _, key = task_id.split(":", 1)
        card_obj = card_map.get(key)
        if card_obj:
            card_obj.set_running(status == "running")

    async def on_log(text: str, tag: str):
        if tag in ("ok", "fail"):
            completed[0] += 1
            pct = completed[0] / total
            progress_bar.value = pct
            progress_text.text = f'Выполняется: {completed[0]} / {total}'

            elapsed = time.time() - start_time
            if completed[0] > 0:
                per_task = elapsed / completed[0]
                remaining = int(per_task * (total - completed[0]))
            else:
                remaining = est_total
            eta_label.text = f'Осталось: {_fmt_eta(remaining)}'
        task_label.text = text

    old_log, old_status = r._log, r._status
    r._log, r._status = on_log, on_status

    async def on_complete():
        progress_bar.value = 1.0
        elapsed = time.time() - start_time
        progress_text.text = f'Готово: {total} / {total}'
        eta_label.text = f'Заняло: {_fmt_eta(int(elapsed))}'
        spinner_el.set_content('<div style="width:20px;height:20px;color:var(--success);">✓</div>')
        btn.enable()
        for card in cards:
            card.set_running(False)
        r._log, r._status = old_log, old_status

    await r.run_tasks(tasks, on_complete=on_complete)
