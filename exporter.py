"""
Экспорт данных в xlsx с примечаниями (история цен, скидки).

Колонки:
  Город | Застройщик | ЖК | Корпус | Кол-во кладовок |
  Номер кладовой | Площадь (м²) | Цена (₽) | Цена/м² (₽) | Ссылка

Без объединения ячеек — чтобы Excel-сортировка работала.
ЖК визуально разделены жирной нижней границей.
"""
from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

from openpyxl import Workbook
from openpyxl.comments import Comment
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

from parsers.base import (
    StorehouseItem, get_price_history, logger,
    PROJECT_DIR,
)

OUTPUT_DIR = PROJECT_DIR / "output"

# Колонки
COLUMNS = [
    "Город",
    "Застройщик",
    "ЖК",
    "Корпус",
    "Кол-во кладовок",
    "Номер кладовой",
    "Площадь (м²)",
    "Цена (₽)",
    "Цена/м² (₽)",
    "Ссылка",
]

# Индексы колонок (1-based)
COL_CITY = 1
COL_DEV = 2
COL_COMPLEX = 3
COL_BUILDING = 4
COL_COUNT = 5
COL_NUMBER = 6
COL_AREA = 7
COL_PRICE = 8
COL_PPM = 9
COL_LINK = 10

# ── Стили ────────────────────────────────────────────
HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
HEADER_FONT = Font(name="Calibri", bold=True, color="FFFFFF", size=11)

DATA_FONT = Font(name="Calibri", size=11)
DATA_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)

LINK_FONT = Font(name="Calibri", size=11, color="0563C1", underline="single")

TOTAL_FONT = Font(name="Calibri", size=11, bold=True)
COMPLEX_TOTAL_FILL = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
GRAND_TOTAL_FILL = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")

# Обычная тонкая граница
THIN_BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)

# Жирная нижняя граница — разделитель между ЖК (на всех ячейках строки)
THICK_BOTTOM = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="medium", color="4472C4"),
)

# Жирная нижняя граница — разделитель корпусов (только на столбце «Корпус»)
BUILDING_BOTTOM = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="medium", color="A9A9A9"),
)

# Маппинг site_key → отображаемое имя
SITE_NAMES = {
    "pik": "ПИК",
    "akbarsdom": "Ак Бар Дом",
    "smu88": "СМУ-88",
    "glorax": "GloraX",
    "unistroy": "УниСтрой",
}


def export_xlsx(
    items: list[StorehouseItem],
    conn: sqlite3.Connection,
    filename: str | None = None,
) -> Path:
    """
    Создать xlsx-файл с данными кладовок.

    Если filename не указан, генерируется по застройщикам:
        storehouses_PIK.xlsx, storehouses_PIK_SMU88.xlsx и т.д.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Генерируем имя файла по застройщикам
    if filename is None:
        sites_in_data = sorted(set(
            SITE_NAMES.get(it.site, it.site).upper().replace(" ", "")
            for it in items
        ))
        suffix = "_".join(sites_in_data) if sites_in_data else "ALL"
        filename = f"storehouses_{suffix}.xlsx"

    output_path = OUTPUT_DIR / filename

    wb = Workbook()
    ws = wb.active
    ws.title = "Кладовки"

    # ── Заголовки ─────────────────────────────────────
    for col_idx, col_name in enumerate(COLUMNS, start=1):
        cell = ws.cell(row=1, column=col_idx, value=col_name)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = THIN_BORDER

    # Столбец «Исх. порядок» — для сброса сортировки
    ord_col = len(COLUMNS) + 1
    ord_header = ws.cell(row=1, column=ord_col, value="Исх.\nпорядок")
    ord_header.fill = HEADER_FILL
    ord_header.font = Font(name="Calibri", bold=True, color="FFFFFF", size=9)
    ord_header.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    ord_header.border = THIN_BORDER

    ws.freeze_panes = "A2"

    # ── Сортировка: город → застройщик → ЖК → корпус → номер ──
    def sort_key(it: StorehouseItem):
        try:
            num = int(it.item_number) if it.item_number else 999999
        except ValueError:
            num = 999999
        return (
            it.city.lower(),
            it.site.lower(),
            it.complex_name.lower(),
            it.building.lower(),
            num,
        )

    sorted_items = sorted(items, key=sort_key)

    # ── Подсчёт кладовок по (site, complex, building) ────
    count_key = lambda it: (it.site, it.complex_name, it.building)
    counts: Counter = Counter(count_key(it) for it in items)

    # ── Определяем границы ЖК и корпусов ─────────────
    complex_key = lambda it: (it.city, it.site, it.complex_name)
    building_key = lambda it: (it.city, it.site, it.complex_name, it.building)

    complex_last_rows: set[int] = set()
    building_last_rows: set[int] = set()  # последняя строка корпуса (но не ЖК)
    for i, item in enumerate(sorted_items):
        is_last = i == len(sorted_items) - 1
        if is_last:
            complex_last_rows.add(i)
        elif complex_key(sorted_items[i]) != complex_key(sorted_items[i + 1]):
            complex_last_rows.add(i)
        elif building_key(sorted_items[i]) != building_key(sorted_items[i + 1]):
            building_last_rows.add(i)

    # ── Заполнение данных (без merge!) ──────────────────
    for i, item in enumerate(sorted_items):
        row_idx = i + 2  # +1 заголовок, +1 индекс
        is_last_in_complex = i in complex_last_rows
        is_last_in_building = i in building_last_rows
        border = THICK_BOTTOM if is_last_in_complex else THIN_BORDER

        display_name = SITE_NAMES.get(item.site, item.site)
        storehouse_count = counts[count_key(item)]

        # Номер — число
        try:
            number_val = int(item.item_number) if item.item_number else ""
        except ValueError:
            number_val = item.item_number or ""

        row_data = [
            item.city,                  # 1 Город
            display_name,               # 2 Застройщик
            item.complex_name,          # 3 ЖК
            item.building,              # 4 Корпус
            storehouse_count,           # 5 Кол-во кладовок
            number_val,                 # 6 Номер кладовой
            item.area,                  # 7 Площадь
            item.price,                 # 8 Цена
            item.price_per_meter,       # 9 Цена/м²
            "Открыть",                  # 10 Ссылка
        ]

        for col_idx, value in enumerate(row_data, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.font = DATA_FONT
            cell.alignment = DATA_ALIGN
            cell.border = border

        # Числовые форматы
        ws.cell(row=row_idx, column=COL_AREA).number_format = '0.0'
        ws.cell(row=row_idx, column=COL_PRICE).number_format = '#,##0'
        ws.cell(row=row_idx, column=COL_PPM).number_format = '#,##0'
        if isinstance(number_val, int):
            ws.cell(row=row_idx, column=COL_NUMBER).number_format = '0'
        ws.cell(row=row_idx, column=COL_COUNT).number_format = '0'

        # Ссылка — гиперлинк
        link_cell = ws.cell(row=row_idx, column=COL_LINK)
        if item.url:
            link_cell.hyperlink = item.url
            link_cell.font = LINK_FONT
        link_cell.alignment = DATA_ALIGN
        link_cell.border = border

        # Разделитель корпусов — жирная граница только на столбце «Корпус»
        if is_last_in_building and not is_last_in_complex:
            ws.cell(row=row_idx, column=COL_BUILDING).border = BUILDING_BOTTOM

        # Столбец «Исх. порядок» — для возврата к исходной сортировке
        ord_cell = ws.cell(row=row_idx, column=COL_LINK + 1, value=i + 1)
        ord_cell.font = Font(name="Calibri", size=9, color="BBBBBB")
        ord_cell.alignment = DATA_ALIGN
        ord_cell.number_format = '0'

        # Примечания с историей цен и скидками
        _add_price_comment(ws, row_idx, COL_PRICE, conn, item)
        _add_ppm_comment(ws, row_idx, COL_PPM, conn, item)

    # ── Итоги по ЖК (внизу) ───────────────────────────
    data_last_row = len(sorted_items) + 1
    row = data_last_row + 2  # пустая строка

    ws.cell(row=row, column=1, value="ИТОГИ ПО ЖК").font = Font(
        name="Calibri", size=12, bold=True
    )
    row += 1

    for col_idx, col_name in enumerate(
        ["Город", "Застройщик", "ЖК", "", "Всего кладовок"], start=1
    ):
        if not col_name:
            continue
        cell = ws.cell(row=row, column=col_idx, value=col_name)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = THIN_BORDER
    row += 1

    # Считаем итоги
    complex_counts: dict[tuple, int] = defaultdict(int)
    for item in items:
        complex_counts[(item.city, item.site, item.complex_name)] += 1

    for (city, site, cname), total in sorted(complex_counts.items()):
        display_name = SITE_NAMES.get(site, site)
        for col, val in [(1, city), (2, display_name), (3, cname), (5, total)]:
            cell = ws.cell(row=row, column=col, value=val)
            cell.font = TOTAL_FONT
            cell.fill = COMPLEX_TOTAL_FILL
            cell.border = THIN_BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center")
        row += 1

    # Общий итог
    total_all = sum(complex_counts.values())
    cell_label = ws.cell(row=row, column=3, value="ВСЕГО")
    cell_label.font = Font(name="Calibri", size=12, bold=True)
    cell_label.alignment = Alignment(horizontal="center")
    cell_total = ws.cell(row=row, column=5, value=total_all)
    cell_total.font = Font(name="Calibri", size=12, bold=True)
    cell_total.fill = GRAND_TOTAL_FILL
    cell_total.border = THIN_BORDER
    cell_total.alignment = Alignment(horizontal="center")

    # ── Ширина колонок ────────────────────────────────
    col_widths = [14, 16, 18, 14, 18, 16, 14, 16, 16, 12, 8]
    for i, w in enumerate(col_widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    # ── Автофильтр (включая столбец «Исх. порядок») ──
    last_col = len(COLUMNS) + 1  # +1 для «Исх. порядок»
    if data_last_row > 1:
        ws.auto_filter.ref = f"A1:{get_column_letter(last_col)}{data_last_row}"

    wb.save(output_path)
    logger.info("xlsx сохранён: %s (%d кладовок)", output_path, len(items))
    return output_path


# ── Примечания ───────────────────────────────────────────

def _add_price_comment(
    ws, row: int, col: int,
    conn: sqlite3.Connection,
    item: StorehouseItem,
) -> None:
    """Примечание к ячейке «Цена» — скидка + история."""
    lines: list[str] = []

    if item.discount_percent and item.original_price:
        lines.append(
            f"Скидка {item.discount_percent:.0f}%, "
            f"цена без скидки: {item.original_price:,.0f} ₽"
        )

    history = get_price_history(conn, item.site, item.item_id)
    if len(history) > 1:
        lines.append("Предыдущие цены:")
        for price, ppm, orig_price, discount, date_str in history[1:]:
            date_short = date_str[:10]
            entry = f"• {price:,.0f} ₽ ({date_short})"
            if discount and orig_price:
                entry += f" [скидка {discount:.0f}%, без скидки: {orig_price:,.0f} ₽]"
            lines.append(entry)

    if lines:
        cell = ws.cell(row=row, column=col)
        cell.comment = Comment("\n".join(lines), "Парсер кладовок")
        cell.comment.width = 350
        cell.comment.height = max(80, len(lines) * 20)


def _add_ppm_comment(
    ws, row: int, col: int,
    conn: sqlite3.Connection,
    item: StorehouseItem,
) -> None:
    """Примечание к ячейке «Цена/м²» — история."""
    history = get_price_history(conn, item.site, item.item_id)
    if len(history) <= 1:
        return

    lines = ["Предыдущие цены/м²:"]
    for price, ppm, orig_price, discount, date_str in history[1:]:
        date_short = date_str[:10]
        lines.append(f"• {ppm:,.0f} ₽/м² ({date_short})")

    cell = ws.cell(row=row, column=col)
    cell.comment = Comment("\n".join(lines), "Парсер кладовок")
    cell.comment.width = 300
    cell.comment.height = max(60, len(lines) * 18)
