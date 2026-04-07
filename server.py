#!/usr/bin/env python3
"""
PIK EVA — Визуализатор (NiceGUI web-сервер).

Запуск: python server.py → http://localhost:8080

Web-интерфейс для оркестранта с liquid glass эстетикой.
Страницы:
  - Сбор: первичный сбор данных парсерами
  - Обновление: обновление БД + автозапуск
  - EVA: генерация расчет_ева.xlsx
  - Формулы: редактор разбалловки (configs/eva.yaml)
  - Логи: журнал выполнения
"""
from nicegui import ui

from gui.layout import build_layout


@ui.page('/')
def main_page():
    build_layout()


if __name__ == "__main__":
    ui.run(
        title="PIK EVA",
        port=8080,
        reload=False,
        dark=None,
        favicon='🏠',
    )
