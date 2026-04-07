"""
PIK EVA GUI — Layout with sidebar and content routing.
"""
from __future__ import annotations

from nicegui import ui

from gui.pages.log_page import LogPage
from gui.runner import TaskRunner
from gui.theme import inject_theme


def build_layout():
    """Build the main layout with sidebar navigation and content area."""
    inject_theme()

    # Shared state
    log_page = LogPage()

    async def log_callback(text: str, tag: str):
        await log_page.append(text, tag)

    async def status_callback(task_id: str, status: str):
        pass  # Status handled per-page

    runner = TaskRunner(log_callback, status_callback)

    current_page = {'value': 'collect'}
    content_ref = {'container': None}

    menu_items = [
        ('play_arrow',  'Сбор',        'collect'),
        ('sync',        'Обновление',  'update'),
        ('bar_chart',   'EVA',         'eva'),
        ('tune',        'Формулы',     'scoring'),
        ('terminal',    'Логи',        'log'),
    ]

    buttons: dict[str, ui.button] = {}

    def switch_page(page_id: str):
        current_page['value'] = page_id
        for pid, btn in buttons.items():
            if pid == page_id:
                btn.classes(add='sidebar-btn-active')
            else:
                btn.classes(remove='sidebar-btn-active')
        _render_content(page_id, content_ref['container'], runner, log_page)

    # ── Sidebar ──
    with ui.left_drawer(value=True, fixed=True).props('width=220 bordered'):

        with ui.column().classes('w-full h-full p-4 gap-1'):
            # Logo
            ui.label('PIK EVA').style(
                'font-size: 20px; font-weight: 700; color: var(--primary); '
                'letter-spacing: -0.03em; padding: 8px 4px 4px;'
            )
            ui.label('Оркестрант').style(
                'font-size: 11px; color: var(--text-muted); padding: 0 4px 16px;'
            )

            # Nav buttons
            for icon, label, page_id in menu_items:
                btn = ui.button(label, icon=icon,
                                on_click=lambda pid=page_id: switch_page(pid)) \
                    .classes('sidebar-btn') \
                    .props('flat no-caps align=left')
                buttons[page_id] = btn

            # Spacer
            ui.space()

            # Theme toggle
            ui.separator().style('border-color: var(--separator);')
            dm = ui.dark_mode(True)

            def toggle_theme(e):
                dm.set_value(e.value)

            ui.switch('Тёмная тема', value=True, on_change=toggle_theme) \
                .style('color: var(--text-secondary); padding: 8px 4px;')

            ui.label('v1.0').style(
                'font-size: 11px; color: var(--text-muted); padding: 4px;'
            )

    # ── Content area ──
    with ui.column().classes('w-full').style(
        'padding: 32px 40px; min-height: 100vh;'
    ) as content:
        content_ref['container'] = content

    switch_page('collect')


def _render_content(page_id: str, container, runner: TaskRunner, log_page: LogPage):
    """Clear content container and render the selected page."""
    if container is None:
        return
    container.clear()
    with container:
        if page_id == 'collect':
            from gui.pages.collect_page import collect_page
            collect_page(runner)
        elif page_id == 'update':
            from gui.pages.update_page import update_page
            update_page(runner)
        elif page_id == 'eva':
            from gui.pages.eva_page import eva_page
            eva_page(runner)
        elif page_id == 'scoring':
            from gui.pages.scoring_page import scoring_page
            scoring_page()
        elif page_id == 'log':
            log_page.build()
