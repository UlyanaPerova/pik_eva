"""
Экспорт данных в xlsx с примечаниями (история цен, скидки).

Оформление:
  - Строки-заголовки для Города и ЖК (объединённые на всю ширину)
  - Данные без merge — сортировка Excel работает
  - Жирная граница между корпусами (на столбце «Корпус»)
  - Секция в примечании столбца «Корпус»
  - Столбец «Исх. порядок» для сброса сортировки
  - Автофильтр
  - Итоги по ЖК внизу

Колонки данных:
  Корпус | Кол-во кладовок | Номер кладовой | Площадь (м²) | Цена (₽) | Цена/м² (₽) | Ссылка
"""
from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

from openpyxl import Workbook
from openpyxl.comments import Comment
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side, numbers
from openpyxl.utils import get_column_letter

from parsers.base import (
    StorehouseItem, get_price_history, logger,
    PROJECT_DIR,
)

OUTPUT_DIR = PROJECT_DIR / "output"

# ── Колонки (только данные, без Город/Застройщик/ЖК — они в строках-заголовках) ──
DATA_COLUMNS = [
    "Корпус",
    "Кол-во\nкладовок",
    "Номер\nкладовой",
    "Площадь\n(м²)",
    "Цена (₽)",
    "Цена/м²\n(₽)",
    "Ссылка",
    "Исх.\nпорядок",
]

NUM_DATA_COLS = len(DATA_COLUMNS)

# Индексы колонок (1-based)
COL_BUILDING = 1
COL_COUNT = 2
COL_NUMBER = 3
COL_AREA = 4
COL_PRICE = 5
COL_PPM = 6
COL_LINK = 7
COL_ORD = 8

# ── Стили ────────────────────────────────────────────
HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
HEADER_FONT = Font(name="Calibri", bold=True, color="FFFFFF", size=11)

CITY_FILL = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
CITY_FONT = Font(name="Calibri", bold=True, color="FFFFFF", size=14)

COMPLEX_FILL = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
COMPLEX_FONT = Font(name="Calibri", bold=True, color="2F5496", size=12)

DATA_FONT = Font(name="Calibri", size=11)
DATA_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)

LINK_FONT = Font(name="Calibri", size=11, color="0563C1", underline="single")

TOTAL_FONT = Font(name="Calibri", size=11, bold=True)
COMPLEX_TOTAL_FILL = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
GRAND_TOTAL_FILL = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")

THIN_BORDER = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="thin", color="D9D9D9"),
)

# Разделитель корпусов — серая граница снизу на столбце «Корпус»
BUILDING_BOTTOM = Border(
    left=Side(style="thin", color="D9D9D9"),
    right=Side(style="thin", color="D9D9D9"),
    top=Side(style="thin", color="D9D9D9"),
    bottom=Side(style="medium", color="808080"),
)

# Маппинг site_key → отображаемое имя
SITE_NAMES = {
    "pik": "ПИК",
    "akbarsdom": "Ак Бар Дом",
    "smu88": "СМУ-88",
    "glorax": "GloraX",
    "unistroy": "УниСтрой",
}

# Латинские ключи для имён файлов (без кириллицы)
SITE_FILE_KEYS = {
    "pik": "PIK",
    "akbarsdom": "AkBarsDom",
    "smu88": "SMU88",
    "glorax": "GloraX",
    "unistroy": "Unistroy",
}


def export_xlsx(
    items: list[StorehouseItem],
    conn: sqlite3.Connection,
    filename: str | None = None,
) -> Path:
    """
    Создать xlsx-файл с данными кладовок.

    Структура:
        [Строка-заголовок ГОРОД]       — объединена на всю ширину
        [Строка-заголовок ЖК]          — объединена на всю ширину
        [Шапка колонок данных]
        [Данные кладовок]              — сортируемые, без merge
        ...
        [Итоги по ЖК]
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Имя файла
    if filename is None:
        sites_in_data = sorted(set(
            SITE_FILE_KEYS.get(it.site, it.site)
            for it in items
        ))
        suffix = "_".join(sites_in_data) if sites_in_data else "ALL"
        filename = f"storehouses_{suffix}.xlsx"

    output_path = OUTPUT_DIR / filename

    wb = Workbook()
    ws = wb.active
    ws.title = "Кладовки"

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

    # ── Группировка: город → (site, ЖК) → кладовки ──
    city_groups: dict[str, dict[tuple[str, str], list[StorehouseItem]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for item in sorted_items:
        city_groups[item.city][(item.site, item.complex_name)].append(item)

    # Подсчёт кладовок по корпусу
    count_key = lambda it: (it.site, it.complex_name, it.building)
    counts: Counter = Counter(count_key(it) for it in items)

    # ── Заполнение ──
    row = 1
    data_start_rows: list[int] = []  # начала блоков данных (для автофильтра)
    data_ranges: list[tuple[int, int]] = []  # (start, end) строк данных

    for city in sorted(city_groups.keys()):
        # ── Строка ГОРОД ──
        city_cell = ws.cell(row=row, column=1, value=city.upper())
        city_cell.font = CITY_FONT
        city_cell.fill = CITY_FILL
        city_cell.alignment = Alignment(horizontal="center", vertical="center")
        ws.merge_cells(
            start_row=row, start_column=1,
            end_row=row, end_column=NUM_DATA_COLS,
        )
        # Заливка на все ячейки мержа
        for c in range(1, NUM_DATA_COLS + 1):
            ws.cell(row=row, column=c).fill = CITY_FILL
        ws.row_dimensions[row].height = 30
        row += 1

        for (site, complex_name) in sorted(city_groups[city].keys()):
            complex_items = city_groups[city][(site, complex_name)]
            display_name = SITE_NAMES.get(site, site)
            total_in_complex = len(complex_items)

            # ── Строка ЖК ──
            jk_text = f"{display_name}  —  {complex_name}  ({total_in_complex} кладовок)"
            jk_cell = ws.cell(row=row, column=1, value=jk_text)
            jk_cell.font = COMPLEX_FONT
            jk_cell.fill = COMPLEX_FILL
            jk_cell.alignment = Alignment(horizontal="center", vertical="center")
            ws.merge_cells(
                start_row=row, start_column=1,
                end_row=row, end_column=NUM_DATA_COLS,
            )
            for c in range(1, NUM_DATA_COLS + 1):
                ws.cell(row=row, column=c).fill = COMPLEX_FILL
            ws.row_dimensions[row].height = 25
            row += 1

            # ── Шапка колонок ──
            for col_idx, col_name in enumerate(DATA_COLUMNS, start=1):
                cell = ws.cell(row=row, column=col_idx, value=col_name)
                cell.fill = HEADER_FILL
                cell.font = HEADER_FONT
                cell.alignment = Alignment(
                    horizontal="center", vertical="center", wrap_text=True
                )
                cell.border = THIN_BORDER
            row += 1

            block_start = row

            # ── Строки данных ──
            for i, item in enumerate(complex_items):
                is_last = i == len(complex_items) - 1
                is_last_in_building = (
                    is_last
                    or complex_items[i].building != complex_items[i + 1].building
                )

                storehouse_count = counts[count_key(item)]

                # Номер — число
                try:
                    number_val = int(item.item_number) if item.item_number else ""
                except ValueError:
                    number_val = item.item_number or ""

                row_data = [
                    item.building,          # 1 Корпус
                    storehouse_count,       # 2 Кол-во кладовок
                    number_val,             # 3 Номер кладовой
                    item.area,              # 4 Площадь
                    item.price,             # 5 Цена
                    item.price_per_meter,   # 6 Цена/м²
                    "Открыть",              # 7 Ссылка
                    row - block_start + 1,  # 8 Исх. порядок
                ]

                for col_idx, value in enumerate(row_data, start=1):
                    cell = ws.cell(row=row, column=col_idx, value=value)
                    cell.font = DATA_FONT
                    cell.alignment = DATA_ALIGN
                    cell.border = THIN_BORDER

                # Числовые форматы
                ws.cell(row=row, column=COL_AREA).number_format = '0.0'
                ws.cell(row=row, column=COL_PRICE).number_format = '#,##0'
                ws.cell(row=row, column=COL_PPM).number_format = '#,##0'
                if isinstance(number_val, int):
                    ws.cell(row=row, column=COL_NUMBER).number_format = '0'
                ws.cell(row=row, column=COL_COUNT).number_format = '0'
                ws.cell(row=row, column=COL_ORD).number_format = '0'
                ws.cell(row=row, column=COL_ORD).font = Font(
                    name="Calibri", size=9, color="BBBBBB"
                )

                # Ссылка — гиперлинк
                link_cell = ws.cell(row=row, column=COL_LINK)
                if item.url:
                    link_cell.hyperlink = item.url
                    link_cell.font = LINK_FONT
                link_cell.alignment = DATA_ALIGN

                # Разделитель корпусов — жирная граница на столбце «Корпус»
                if is_last_in_building and not is_last:
                    ws.cell(row=row, column=COL_BUILDING).border = BUILDING_BOTTOM

                # Примечание «Корпус» — секция
                section_name = getattr(item, '_section_name', None)
                if section_name:
                    building_cell = ws.cell(row=row, column=COL_BUILDING)
                    building_cell.comment = Comment(
                        section_name, "Парсер кладовок"
                    )
                    building_cell.comment.width = 150
                    building_cell.comment.height = 30

                # Примечания с историей цен и скидками
                _add_price_comment(ws, row, COL_PRICE, conn, item)
                _add_ppm_comment(ws, row, COL_PPM, conn, item)

                row += 1

            block_end = row - 1
            data_ranges.append((block_start, block_end))

    # ── Итоги по ЖК ───────────────────────────────────
    row += 1  # пустая строка

    ws.cell(row=row, column=1, value="ИТОГИ ПО ЖК").font = Font(
        name="Calibri", size=12, bold=True
    )
    row += 1

    summary_headers = ["Город", "Застройщик", "ЖК", "Всего"]
    for col_idx, col_name in enumerate(summary_headers, start=1):
        cell = ws.cell(row=row, column=col_idx, value=col_name)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = THIN_BORDER
    row += 1

    complex_counts: dict[tuple, int] = defaultdict(int)
    for item in items:
        complex_counts[(item.city, item.site, item.complex_name)] += 1

    for (city, site, cname), total in sorted(complex_counts.items()):
        display_name = SITE_NAMES.get(site, site)
        for col, val in [(1, city), (2, display_name), (3, cname), (4, total)]:
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
    cell_total = ws.cell(row=row, column=4, value=total_all)
    cell_total.font = Font(name="Calibri", size=12, bold=True)
    cell_total.fill = GRAND_TOTAL_FILL
    cell_total.border = THIN_BORDER
    cell_total.alignment = Alignment(horizontal="center")

    # ── Ширина колонок ────────────────────────────────
    col_widths = [16, 14, 14, 12, 14, 14, 12, 8]
    for i, w in enumerate(col_widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    # ── Автофильтр на каждый блок данных ──
    # Openpyxl поддерживает только один автофильтр на лист.
    # Ставим на самый большой блок данных.
    if data_ranges:
        biggest = max(data_ranges, key=lambda r: r[1] - r[0])
        # Фильтр охватывает шапку (строка выше блока) + данные
        header_row = biggest[0] - 1
        ws.auto_filter.ref = (
            f"A{header_row}:{get_column_letter(NUM_DATA_COLS)}{biggest[1]}"
        )

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
