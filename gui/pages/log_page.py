"""
PIK EVA GUI — Log Page (Журнал выполнения).
"""
from nicegui import ui


class LogPage:
    """Shared log widget accessible from all pages."""

    def __init__(self):
        self._log_widget: ui.log | None = None
        self._entries: list[tuple[str, str]] = []

    async def append(self, text: str, tag: str = "info"):
        """Add a log entry. tag: ok, fail, info."""
        from datetime import datetime
        ts = datetime.now().strftime("%H:%M:%S")
        self._entries.append((f"[{ts}] {text}", tag))
        if self._log_widget is not None:
            self._log_widget.push(f"[{ts}] {text}")

    def build(self):
        """Build the log page UI."""
        with ui.column().classes('w-full gap-4 animate-in'):
            with ui.row().classes('w-full justify-between items-center'):
                ui.label('Журнал выполнения').classes('text-heading')
                ui.button('Очистить', icon='delete_outline', on_click=self._clear) \
                    .props('flat no-caps') \
                    .style('color: var(--text-muted);')

            self._log_widget = ui.log(max_lines=500).style(
                'width: 100%; height: 600px;'
            )

            for text, _tag in self._entries:
                self._log_widget.push(text)

    def _clear(self):
        self._entries.clear()
        if self._log_widget is not None:
            self._log_widget.clear()
