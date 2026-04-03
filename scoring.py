"""
Модуль скоринга ЕВА — единый источник истины для всех формул баллов.

Архитектура:
  configs/eva.yaml → scoring.py → eva_calculator.py

Для плагина управления:
  1. Пользователь редактирует пороги в configs/eva.yaml
  2. scoring.py читает пороги и генерирует Excel-формулы
  3. eva_calculator.py вызывает scoring.py для вставки формул в XLSX

Два публичных API:
  - generate_first_stage_formula(row)  → Excel-формула первого этапа
  - generate_second_stage_formula(row) → Excel-формула второго этапа
  - calc_first_stage(...)              → Python-расчёт (валидация)
  - calc_second_stage(...)             → Python-расчёт (валидация)

Первый этап (лист «жк», 11 критериев):
  1. Срок ввода (дни до сдачи)
  2. Соотношение квартир / кладовых
  3. Квартирография: студии + 1к
  4. Квартирография: 2к (зависит от балконов)
  5. Квартирография: 3к + 4к (зависит от балконов)
  6. Средняя площадь по типам квартир
  7. Средняя нежилая площадь по типам квартир
  8. Соотношение квартир к балконам (ручной)
  9. Размер балконов (ручной)
  10. Локация ЖК (ручной)
  11. Удобство доступа (ручной)

Второй этап (лист «кладовки», 4 критерия):
  1. Площадь кладовки
  2. Стоимость кладовки
  3. Цена за м² кладовки
  4. Соотношение цены квартир / кладовок
"""
from __future__ import annotations

from pathlib import Path

import yaml


CONFIGS_DIR = Path(__file__).resolve().parent / "configs"


def load_scoring_config() -> dict:
    """Загрузить конфиг скоринга из eva.yaml."""
    path = CONFIGS_DIR / "eva.yaml"
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("scoring", {})


# ═══════════════════════════════════════════════════════
#  PYTHON-РАСЧЁТ (для валидации и предпросмотра)
# ═══════════════════════════════════════════════════════

def _score_by_thresholds(value: float, thresholds: list[dict]) -> int:
    """Пороговая оценка: если value < max → points баллов."""
    for t in thresholds:
        if value < t["max"]:
            return t["points"]
    return thresholds[-1]["points"] if thresholds else 0


def calc_first_stage(
    days_until: int | None,
    rooms_count: dict,
    avg_area: dict,
    avg_non_living: dict,
    scoring: dict | None = None,
) -> int:
    """Первый этап: критерии 1–7 (автоматические).
    Критерии 8–11 зависят от ручного ввода → рассчитываются в Excel."""
    if scoring is None:
        scoring = load_scoring_config()
    pts = 0

    # 1. Срок ввода
    if days_until is not None:
        months = days_until / 30.0
        pts += _score_by_thresholds(months, scoring["deadline_months"])

    # 4. Квартирография
    total = sum(rooms_count.values()) if rooms_count else 0
    if total > 0:
        kvart = scoring.get("kvartirografia", {})
        s1k = (rooms_count.get(0, 0) + rooms_count.get(1, 0)) / total * 100
        two_k = rooms_count.get(2, 0) / total * 100
        three_4k = (rooms_count.get(3, 0) + rooms_count.get(4, 0)) / total * 100

        if "studio_1k" in kvart:
            pts += _score_by_thresholds(s1k, kvart["studio_1k"])
        if "two_k" in kvart:
            pts += _score_by_thresholds(two_k, kvart["two_k"])
        if "three_4k" in kvart:
            pts += _score_by_thresholds(three_4k, kvart["three_4k"])

    # 5. Средняя площадь
    area_cfg = scoring.get("avg_area", {})
    room_keys = [(0, "studio"), (1, "one_k"), (2, "two_k"), (3, "three_k"), (4, "four_k")]
    for rooms, cfg_key in room_keys:
        avg = avg_area.get(rooms, 0)
        if avg > 0 and cfg_key in area_cfg:
            pts += _score_by_thresholds(avg, area_cfg[cfg_key])

    # 6. Средняя нежилая площадь
    nl_cfg = scoring.get("avg_non_living", {})
    for rooms, cfg_key in room_keys:
        avg_nl = avg_non_living.get(rooms, 0)
        if avg_nl > 0 and cfg_key in nl_cfg:
            pts += _score_by_thresholds(avg_nl, nl_cfg[cfg_key])

    return pts


def calc_second_stage(
    area: float,
    price: float,
    price_per_meter: float,
    scoring: dict | None = None,
) -> int:
    """Второй этап: критерии площади, стоимости, цены/м² кладовки."""
    if scoring is None:
        scoring = load_scoring_config()
    pts = 0
    if area > 0:
        pts += _score_by_thresholds(area, scoring.get("storehouse_area", []))
    if price > 0:
        pts += _score_by_thresholds(price, scoring.get("storehouse_price", []))
    if price_per_meter > 0:
        pts += _score_by_thresholds(price_per_meter, scoring.get("storehouse_ppm", []))
    return pts


# ═══════════════════════════════════════════════════════
#  МАППИНГ СТОЛБЦОВ
# ═══════════════════════════════════════════════════════

# Лист «жк» (1-indexed). При изменении порядка столбцов —
# обновить ТОЛЬКО этот словарь.
JK_COLS = {
    "city": "A",              # 1
    "developer": "B",         # 2
    "complex": "C",           # 3
    "building": "D",          # 4
    "days": "E",              # 5
    "domrf_link": "F",        # 6
    "dev_link": "G",          # 7
    "first_stage": "H",       # 8
    "balcony_count": "I",     # 9
    "balcony_size": "J",      # 10
    "location": "K",          # 11
    "access": "L",            # 12
    "apt_store_ratio": "M",   # 13
    "balcony_ratio": "N",     # 14
    "apt_count": "O",         # 15
    "store_domrf": "P",       # 16
    "store_dev": "Q",         # 17
    "avg_apt_ppm": "R",       # 18
    "rooms_studio": "S",      # 19
    "rooms_1k": "T",          # 20
    "rooms_2k": "U",          # 21
    "rooms_3k": "V",          # 22
    "rooms_4k": "W",          # 23
    "area_studio": "X",       # 24
    "area_1k": "Y",           # 25
    "area_2k": "Z",           # 26
    "area_3k": "AA",          # 27
    "area_4k": "AB",          # 28
    "nl_studio": "AC",        # 29
    "nl_1k": "AD",            # 30
    "nl_2k": "AE",            # 31
    "nl_3k": "AF",            # 32
    "nl_4k": "AG",            # 33
}

# Лист «кладовки (застройщик)»
STORE_COLS = {
    "ratio_price": "G",       # 7: соотнош. цены м² квартир/кладовых
    "area": "L",              # 12
    "price": "M",             # 13
    "ppm": "N",               # 14
}


# ═══════════════════════════════════════════════════════
#  ГЕНЕРАЦИЯ EXCEL-ФОРМУЛ ИЗ eva.yaml
# ═══════════════════════════════════════════════════════

def _excel_threshold_formula(cell_ref: str, thresholds: list[dict]) -> str:
    """Сгенерировать вложенный IF из списка порогов eva.yaml.

    thresholds = [{max: 6, points: 20}, {max: 12, points: 15}, ...]
    → IF(cell<6,20,IF(cell<12,15,...))
    """
    if not thresholds:
        return "0"
    parts = []
    for t in thresholds:
        parts.append(f'IF({cell_ref}<{t["max"]},{t["points"]},')
    # Последнее значение — points последнего порога
    last = str(thresholds[-1]["points"])
    return "".join(parts) + last + ")" * len(parts)


def _excel_map_formula(cell_ref: str, mapping: dict[str, int]) -> str:
    """Сгенерировать вложенный IF из словаря строка→баллы.

    mapping = {"Центр": 5, "Норм": 3, ...}
    → IF(cell="Центр",5,IF(cell="Норм",3,...,0))
    """
    if not mapping:
        return "0"
    parts = []
    for label, pts in mapping.items():
        parts.append(f'IF({cell_ref}="{label}",{pts},')
    return "".join(parts) + "0" + ")" * len(parts)


def generate_first_stage_formula(row: int, scoring: dict | None = None) -> str:
    """Генерирует Excel-формулу ПЕРВОГО ЭТАПА из порогов eva.yaml.

    Пользователь меняет пороги в eva.yaml → формула меняется автоматически.
    """
    if scoring is None:
        scoring = load_scoring_config()

    r = row
    c = JK_COLS

    # Защита: если данных нет — 0
    guard = (f'IF(OR({c["complex"]}{r}="",'
             f'{c["building"]}{r}="",'
             f'COUNTA({c["apt_store_ratio"]}{r}:{c["nl_4k"]}{r})<15),0,IFERROR(')

    # ── 1. Срок ввода (дни) ──
    # Линейная интерполяция (не из yaml — специфическая формула)
    f1 = (f'(IF(OR({c["days"]}{r}="",{c["days"]}{r}=0),0,'
          f'IF({c["days"]}{r}<365,20-{c["days"]}{r}*(10/365),'
          f'IF({c["days"]}{r}<730,10-({c["days"]}{r}-365)*(5/365),0))))')

    # ── 2. Соотнош. квартир/кладовых (M) ──
    # Нелинейная шкала (не из yaml — специфическая формула)
    m = c["apt_store_ratio"]
    f2 = (f'(IF({m}{r}<5,-10-(5-{m}{r})*2,'
          f'IF({m}{r}<=10,15+({m}{r}-5)*5,'
          f'IF({m}{r}<=15,40+({m}{r}-10)*10,'
          f'90+({m}{r}-15)*15))))')

    # ── 3. Квартирография: студии + 1к (%) ── из eva.yaml
    s = c["rooms_studio"]
    t = c["rooms_1k"]
    kvart = scoring.get("kvartirografia", {})
    f3_inner = _excel_threshold_formula(f'({s}{r}+{t}{r})', kvart.get("studio_1k", []))
    f3 = f'({f3_inner})'

    # ── 4. Квартирография: 2к (%) ── зависит от балконов
    u = c["rooms_2k"]
    n = c["balcony_ratio"]
    j = c["balcony_size"]
    two_k_th = kvart.get("two_k", [])
    f4_base = _excel_threshold_formula(f'{u}{r}', two_k_th)
    # Бонусы при хороших балконах (захардкожены — специфическая логика)
    f4 = (f'(IF(AND({n}{r}>=1.5,{n}{r}<=2,{j}{r}="М"),'
          f'IF({u}{r}<40,1,IF({u}{r}<=50,3,5)),'
          f'IF(AND({n}{r}>2,OR({j}{r}="М",{j}{r}="С")),'
          f'IF({u}{r}<40,2,IF({u}{r}<=50,5,7)),'
          f'{f4_base})))')

    # ── 5. Квартирография: 3к + 4к (%) ── зависит от балконов
    v = c["rooms_3k"]
    w = c["rooms_4k"]
    three_4k_th = kvart.get("three_4k", [])
    f5_base = _excel_threshold_formula(f'({v}{r}+{w}{r})', three_4k_th)
    f5 = (f'(IF(AND({n}{r}>=1.5,{n}{r}<=2,{j}{r}="М"),'
          f'IF(({v}{r}+{w}{r})<10,2,IF(({v}{r}+{w}{r})<=15,5,10)),'
          f'IF(AND({n}{r}>2,OR({j}{r}="М",{j}{r}="С")),'
          f'IF(({v}{r}+{w}{r})<10,5,IF(({v}{r}+{w}{r})<=15,10,15)),'
          f'{f5_base})))')

    # ── 6. Средняя площадь по типам ── из eva.yaml
    area_cfg = scoring.get("avg_area", {})
    area_cols = ["area_studio", "area_1k", "area_2k", "area_3k", "area_4k"]
    area_keys = ["studio", "one_k", "two_k", "three_k", "four_k"]
    f6_parts = []
    for col_name, cfg_key in zip(area_cols, area_keys):
        cl = c[col_name]
        th = area_cfg.get(cfg_key, [])
        inner = _excel_threshold_formula(f'{cl}{r}', th) if th else "0"
        f6_parts.append(f'IF(N({cl}{r})>0,{inner},0)')
    f6 = f'({"+".join(f6_parts)})'

    # ── 7. Нежилая площадь по типам ── из eva.yaml
    nl_cfg = scoring.get("avg_non_living", {})
    nl_cols = ["nl_studio", "nl_1k", "nl_2k", "nl_3k", "nl_4k"]
    nl_keys = ["studio", "one_k", "two_k", "three_k", "four_k"]
    f7_parts = []
    for col_name, cfg_key in zip(nl_cols, nl_keys):
        cl = c[col_name]
        th = nl_cfg.get(cfg_key, [])
        inner = _excel_threshold_formula(f'{cl}{r}', th) if th else "0"
        f7_parts.append(f'IF({cl}{r}>0,{inner},0)')
    f7 = f'({"+".join(f7_parts)})'

    # ── 8. Балконы: ratio (N) ── нелинейная шкала
    f8 = (f'(IF({n}{r}<=1,-10,IF({n}{r}<=1.5,5,IF({n}{r}<=2,10+({n}{r}-1.5)*10,'
          f'IF({n}{r}<=2.5,15+({n}{r}-2)*15,IF({n}{r}<=3,22.5+({n}{r}-2.5)*25,'
          f'IF({n}{r}<=3.5,35+({n}{r}-3)*30,IF({n}{r}<=4,50+({n}{r}-3.5)*35,'
          f'67.5+({n}{r}-4)*40))))))))')

    # ── 9. Размер балконов (J) ── из eva.yaml
    balcony_map = scoring.get("balcony_size", {})
    f9 = f'({_excel_map_formula(f"{j}{r}", balcony_map)})'

    # ── 10. Локация (K) ── из eva.yaml
    location_map = scoring.get("location", {})
    k = c["location"]
    f10 = f'({_excel_map_formula(f"{k}{r}", location_map)})'

    # ── 11. Доступ (L) ── из eva.yaml
    access_map = scoring.get("access", {})
    l_col = c["access"]
    f11 = f'({_excel_map_formula(f"{l_col}{r}", access_map)})'

    formula = (
        f'=ROUND({guard}'
        f'{f1}+{f2}+{f3}+{f4}+{f5}+{f6}+{f7}+{f8}+{f9}+{f10}+{f11}'
        f',0)),2)'
    )
    return formula


def generate_second_stage_formula(row: int, scoring: dict | None = None) -> str:
    """Генерирует Excel-формулу ВТОРОГО ЭТАПА.

    Критерии:
      1. Площадь кладовки — линейная интерполяция
      2. Стоимость кладовки — линейная интерполяция
      3. Цена/м² кладовки — линейная интерполяция
      4. Соотношение цены квартир / кладовок — линейная интерполяция

    Пороги для линейных формул задаются ниже.
    Для плагина управления: вынести эти пороги в eva.yaml → секция scoring.second_stage.
    """
    if scoring is None:
        scoring = load_scoring_config()

    r = row
    sc = STORE_COLS

    # Пороги второго этапа (можно перенести в eva.yaml)
    ss = scoring.get("second_stage", {})
    # Площадь
    area_min = ss.get("area_min", 2.5)
    area_good = ss.get("area_good", 4.5)
    area_pts = ss.get("area_good_pts", 5)
    area_penalty = ss.get("area_penalty", -20)
    area_decay = ss.get("area_decay_per_m2", 3)
    # Стоимость
    price_max = ss.get("price_max", 650000)
    price_pts = ss.get("price_max_pts", 10)
    price_penalty_threshold = ss.get("price_penalty_threshold", 1000000)
    price_penalty = ss.get("price_penalty", -5)
    # Цена/м²
    ppm_max = ss.get("ppm_max", 110000)
    ppm_pts = ss.get("ppm_max_pts", 20)
    ppm_range = ss.get("ppm_range", 60000)
    # Соотнош. цены
    ratio_pivot = ss.get("ratio_pivot", 3)
    ratio_penalty_mult = ss.get("ratio_penalty_mult", 30)
    ratio_bonus_mult = ss.get("ratio_bonus_mult", 5)

    a = sc["area"]
    p = sc["price"]
    n = sc["ppm"]
    g = sc["ratio_price"]

    formula = (
        f'=ROUND(IF(OR({g}{r}=0,{g}{r}=""),0,IFERROR('
        # 1. Площадь
        f'(IF({a}{r}<{area_min},{area_penalty},'
        f'IF({a}{r}<={area_good},{area_pts},{area_pts}-({a}{r}-{area_good})*{area_decay})))'
        # 2. Стоимость
        f'+(IF({p}{r}>{price_penalty_threshold},{price_penalty},'
        f'({price_max}-{p}{r})*({price_pts}/{price_max})))'
        # 3. Цена/м²
        f'+(IF({n}{r}>{ppm_max},0,({ppm_max}-{n}{r})*({ppm_pts}/{ppm_range})))'
        # 4. Соотнош. цены
        f'+(IF({g}{r}<{ratio_pivot},({g}{r}-{ratio_pivot})*{ratio_penalty_mult},'
        f'({g}{r}-{ratio_pivot})*{ratio_bonus_mult}))'
        f',0)),2)'
    )
    return formula


# ═══════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФОРМУЛЫ
# ═══════════════════════════════════════════════════════

def generate_balcony_ratio_formula(row: int) -> str:
    """Формула: кол-во квартир (O) / кол-во балконов (I)."""
    return f'=IFERROR({JK_COLS["apt_count"]}{row}/{JK_COLS["balcony_count"]}{row},"")'


def generate_total_formula(row: int) -> str:
    """Формула: первый этап + второй этап (лист кладовки, col 11)."""
    return f'=ROUND(I{row}+J{row},2)'
