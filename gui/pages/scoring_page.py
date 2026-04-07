"""
PIK EVA GUI — Scoring Page (Формулы разбалловки).

Tabs: Первый этап | Второй этап | Алиасы
"""
from __future__ import annotations

from nicegui import ui

from gui.widgets.scoring_editor import (
    ThresholdEditor, MapEditor, NumberFields,
    load_scoring, load_full_config, save_scoring, save_aliases, preview_formula,
)


def scoring_page():
    """Build the Scoring page UI."""
    scoring = load_scoring()
    full_cfg = load_full_config()
    editors: list = []

    with ui.column().classes('w-full gap-4 animate-in'):
        ui.label('Формулы разбалловки').classes('text-heading')
        ui.label('Редактирование порогов и формул из configs/eva.yaml').classes('text-caption')

        with ui.tabs().classes('w-full').props('dense') as tabs:
            tab1 = ui.tab('Первый этап')
            tab2 = ui.tab('Второй этап')
            tab3 = ui.tab('Алиасы')

        with ui.tab_panels(tabs, value=tab1).classes('w-full'):

            with ui.tab_panel(tab1):
                _build_first_stage(scoring, editors)

            with ui.tab_panel(tab2):
                _build_second_stage(scoring, editors)

            with ui.tab_panel(tab3):
                _build_aliases(full_cfg, editors)

        # Action bar
        with ui.row().classes('w-full justify-end gap-3 mt-6 pt-4').style(
            'border-top: 1px solid var(--separator);'
        ):
            ui.button('Отменить', on_click=_reload_page) \
                .props('flat no-caps').style('color: var(--text-muted);')
            ui.button('Предпросмотр формулы',
                      on_click=lambda: _show_preview(scoring, editors)) \
                .props('outline no-caps').style(
                    'color: var(--primary); border-color: var(--glass-border);'
                )
            ui.button('Сохранить',
                      on_click=lambda: _save(scoring, editors, full_cfg)) \
                .props('no-caps icon=save').style(
                    'background: var(--primary); color: white; border-radius: var(--radius-sm);'
                )


def _build_first_stage(scoring: dict, editors: list):
    """Build first stage criteria editors."""

    with ui.expansion('1. Срок ввода (дни)', icon='schedule').classes('w-full'):
        dl = scoring.get("deadline_formula", {})
        dl_editor = NumberFields({
            "tier1_days": ("Порог 1 (дни)", dl.get("tier1_days", 365)),
            "tier1_max_pts": ("Макс. баллов (порог 1)", dl.get("tier1_max_pts", 20)),
            "tier1_decay_pts": ("Убывание (порог 1)", dl.get("tier1_decay_pts", 10)),
            "tier2_days": ("Порог 2 (дни)", dl.get("tier2_days", 730)),
            "tier2_max_pts": ("Макс. баллов (порог 2)", dl.get("tier2_max_pts", 10)),
            "tier2_decay_pts": ("Убывание (порог 2)", dl.get("tier2_decay_pts", 5)),
        })
        dl_editor.build()
        editors.append(("deadline_formula", dl_editor))

    with ui.expansion('2. Соотношение кв./кладовых', icon='compare_arrows').classes('w-full'):
        asr = scoring.get("apt_store_ratio_formula", {})
        asr_editor = NumberFields({
            "under_5_base": ("< 5: базовые баллы", asr.get("under_5_base", -10)),
            "under_5_mult": ("< 5: множитель", asr.get("under_5_mult", 2)),
            "tier_5_10_base": ("5-10: базовые баллы", asr.get("tier_5_10_base", 15)),
            "tier_5_10_mult": ("5-10: множитель", asr.get("tier_5_10_mult", 5)),
            "tier_10_15_base": ("10-15: базовые баллы", asr.get("tier_10_15_base", 40)),
            "tier_10_15_mult": ("10-15: множитель", asr.get("tier_10_15_mult", 10)),
            "over_15_base": ("> 15: базовые баллы", asr.get("over_15_base", 90)),
            "over_15_mult": ("> 15: множитель", asr.get("over_15_mult", 15)),
        })
        asr_editor.build()
        editors.append(("apt_store_ratio_formula", asr_editor))

    kvart = scoring.get("kvartirografia", {})

    with ui.expansion('3. Квартирография: студии + 1к', icon='home').classes('w-full'):
        s1k_editor = ThresholdEditor(kvart.get("studio_1k", []))
        s1k_editor.build()
        editors.append(("kvartirografia.studio_1k", s1k_editor))

    with ui.expansion('4. Квартирография: 2к', icon='home').classes('w-full'):
        two_k_editor = ThresholdEditor(kvart.get("two_k", []))
        two_k_editor.build()
        editors.append(("kvartirografia.two_k", two_k_editor))

        bb = scoring.get("balcony_bonus", {})
        with ui.expansion('Бонусы балконов (2к)', icon='balcony').classes('w-full mt-2'):
            bb_2k_editor = NumberFields({
                "two_k_bonus_ratio_range_min": ("Ratio мин.", bb.get("two_k_bonus_ratio_range", [1.5, 2])[0]),
                "two_k_bonus_ratio_range_max": ("Ratio макс.", bb.get("two_k_bonus_ratio_range", [1.5, 2])[1]),
                "two_k_bonus2_ratio_min": ("Бонус 2: ratio мин.", bb.get("two_k_bonus2_ratio_min", 2)),
            })
            bb_2k_editor.build()
            editors.append(("balcony_bonus._2k_fields", bb_2k_editor))

            ui.label('Бонус тир 1:').style(
                'font-size: 13px; color: var(--text-secondary); margin-top: 8px;'
            )
            bb_2k_t1 = ThresholdEditor(bb.get("two_k_bonus_tiers", []))
            bb_2k_t1.build()
            editors.append(("balcony_bonus.two_k_bonus_tiers", bb_2k_t1))

            ui.label('Бонус тир 2:').style(
                'font-size: 13px; color: var(--text-secondary); margin-top: 8px;'
            )
            bb_2k_t2 = ThresholdEditor(bb.get("two_k_bonus2_tiers", []))
            bb_2k_t2.build()
            editors.append(("balcony_bonus.two_k_bonus2_tiers", bb_2k_t2))

    with ui.expansion('5. Квартирография: 3к + 4к', icon='home').classes('w-full'):
        three_4k_editor = ThresholdEditor(kvart.get("three_4k", []))
        three_4k_editor.build()
        editors.append(("kvartirografia.three_4k", three_4k_editor))

        bb = scoring.get("balcony_bonus", {})
        with ui.expansion('Бонусы балконов (3к+4к)', icon='balcony').classes('w-full mt-2'):
            ui.label('Бонус тир 1:').style(
                'font-size: 13px; color: var(--text-secondary);'
            )
            bb_3k_t1 = ThresholdEditor(bb.get("three_4k_bonus_tiers", []))
            bb_3k_t1.build()
            editors.append(("balcony_bonus.three_4k_bonus_tiers", bb_3k_t1))

            ui.label('Бонус тир 2:').style(
                'font-size: 13px; color: var(--text-secondary); margin-top: 8px;'
            )
            bb_3k_t2 = ThresholdEditor(bb.get("three_4k_bonus2_tiers", []))
            bb_3k_t2.build()
            editors.append(("balcony_bonus.three_4k_bonus2_tiers", bb_3k_t2))

    area_cfg = scoring.get("avg_area", {})
    room_types = [
        ("studio", "Студии"), ("one_k", "1к"), ("two_k", "2к"),
        ("three_k", "3к"), ("four_k", "4к"),
    ]

    with ui.expansion('6. Средняя площадь (м\u00b2)', icon='square_foot').classes('w-full'):
        with ui.tabs().classes('w-full').props('dense') as area_tabs:
            for key, label in room_types:
                ui.tab(label)
        with ui.tab_panels(area_tabs, value=room_types[0][1]).classes('w-full'):
            for key, label in room_types:
                with ui.tab_panel(label):
                    ed = ThresholdEditor(area_cfg.get(key, []))
                    ed.build()
                    editors.append((f"avg_area.{key}", ed))

    nl_cfg = scoring.get("avg_non_living", {})

    with ui.expansion('7. Нежилая площадь (м\u00b2)', icon='square_foot').classes('w-full'):
        with ui.tabs().classes('w-full').props('dense') as nl_tabs:
            for key, label in room_types:
                ui.tab(label)
        with ui.tab_panels(nl_tabs, value=room_types[0][1]).classes('w-full'):
            for key, label in room_types:
                with ui.tab_panel(label):
                    ed = ThresholdEditor(nl_cfg.get(key, []))
                    ed.build()
                    editors.append((f"avg_non_living.{key}", ed))

    with ui.expansion('8. Соотношение кв./балконов', icon='aspect_ratio').classes('w-full'):
        brf = scoring.get("balcony_ratio_formula", {})
        bp = brf.get("breakpoints", [])
        bp_as_thresholds = [{"max": b[0], "points": b[1]} for b in bp]
        bp_editor = ThresholdEditor(bp_as_thresholds)
        bp_editor.build()
        editors.append(("balcony_ratio_formula.breakpoints", bp_editor))

    with ui.expansion('9. Размер балконов', icon='open_with').classes('w-full'):
        bs_editor = MapEditor(scoring.get("balcony_size", {}))
        bs_editor.build()
        editors.append(("balcony_size", bs_editor))

    with ui.expansion('10. Локация ЖК', icon='location_on').classes('w-full'):
        loc_editor = MapEditor(scoring.get("location", {}))
        loc_editor.build()
        editors.append(("location", loc_editor))

    with ui.expansion('11. Удобство доступа', icon='elevator').classes('w-full'):
        acc_editor = MapEditor(scoring.get("access", {}))
        acc_editor.build()
        editors.append(("access", acc_editor))


def _build_second_stage(scoring: dict, editors: list):
    """Build second stage criteria editors."""
    ss = scoring.get("second_stage", {})

    with ui.expansion('1. Площадь кладовки', icon='square_foot').classes('w-full'):
        area_ed = NumberFields({
            "area_min": ("Мин. площадь (м\u00b2)", ss.get("area_min", 2.5)),
            "area_good": ("Оптим. площадь (м\u00b2)", ss.get("area_good", 4.5)),
            "area_good_pts": ("Баллы за оптим.", ss.get("area_good_pts", 5)),
            "area_penalty": ("Штраф < мин.", ss.get("area_penalty", -20)),
            "area_decay_per_m2": ("Убыв. за м\u00b2 сверх", ss.get("area_decay_per_m2", 3)),
        })
        area_ed.build()
        editors.append(("second_stage._area", area_ed))

    with ui.expansion('2. Стоимость кладовки', icon='payments').classes('w-full'):
        price_ed = NumberFields({
            "price_max": ("Потолок (\u20bd)", ss.get("price_max", 650000)),
            "price_max_pts": ("Макс. баллов", ss.get("price_max_pts", 10)),
            "price_penalty_threshold": ("Порог штрафа (\u20bd)", ss.get("price_penalty_threshold", 1000000)),
            "price_penalty": ("Штраф", ss.get("price_penalty", -5)),
        })
        price_ed.build()
        editors.append(("second_stage._price", price_ed))

    with ui.expansion('3. Цена за м\u00b2', icon='price_change').classes('w-full'):
        ppm_ed = NumberFields({
            "ppm_max": ("Потолок (\u20bd/м\u00b2)", ss.get("ppm_max", 110000)),
            "ppm_max_pts": ("Макс. баллов", ss.get("ppm_max_pts", 20)),
            "ppm_range": ("Диапазон нормировки", ss.get("ppm_range", 60000)),
        })
        ppm_ed.build()
        editors.append(("second_stage._ppm", ppm_ed))

    with ui.expansion('4. Соотношение цены кв./кладовок', icon='compare').classes('w-full'):
        ratio_ed = NumberFields({
            "ratio_pivot": ("Точка перелома", ss.get("ratio_pivot", 3)),
            "ratio_penalty_mult": ("Множит. штрафа", ss.get("ratio_penalty_mult", 30)),
            "ratio_bonus_mult": ("Множит. бонуса", ss.get("ratio_bonus_mult", 5)),
        })
        ratio_ed.build()
        editors.append(("second_stage._ratio", ratio_ed))


def _build_aliases(full_cfg: dict, editors: list):
    """Build aliases editor."""
    ca = full_cfg.get("complex_aliases", {}) or {}
    with ui.expansion('Алиасы ЖК (complex_aliases)', icon='swap_horiz').classes('w-full'):
        ca_editor = MapEditor(ca)
        ca_editor.build()
        editors.append(("_complex_aliases", ca_editor))

    ba = full_cfg.get("building_aliases", {}) or {}
    with ui.expansion('Алиасы корпусов (building_aliases)', icon='domain').classes('w-full'):
        ba_editors = {}
        for complex_name, mappings in ba.items():
            with ui.expansion(complex_name, icon='apartment').classes('w-full'):
                ed = MapEditor(mappings or {})
                ed.build()
                ba_editors[complex_name] = ed
        editors.append(("_building_aliases", ba_editors))


def _collect_scoring(scoring: dict, editors: list) -> dict:
    """Collect all editor values back into a scoring dict."""
    new_scoring = dict(scoring)

    for path, editor in editors:
        if path.startswith("_"):
            continue

        if isinstance(editor, NumberFields):
            data = editor.get_data()
            if "." in path:
                parent = path.split(".")[0]
                if parent not in new_scoring:
                    new_scoring[parent] = {}
                for k, v in data.items():
                    new_scoring[parent][k] = v
            else:
                new_scoring[path] = data

        elif isinstance(editor, ThresholdEditor):
            data = editor.get_data()
            if "." in path:
                parts = path.split(".")
                if parts[0] == "balcony_ratio_formula" and parts[1] == "breakpoints":
                    bp = [[t["max"], t["points"]] for t in data]
                    if "balcony_ratio_formula" not in new_scoring:
                        new_scoring["balcony_ratio_formula"] = {}
                    new_scoring["balcony_ratio_formula"]["breakpoints"] = bp
                elif len(parts) == 2:
                    if parts[0] not in new_scoring:
                        new_scoring[parts[0]] = {}
                    new_scoring[parts[0]][parts[1]] = data
            else:
                new_scoring[path] = data

        elif isinstance(editor, MapEditor):
            data = editor.get_data()
            if "." in path:
                parts = path.split(".")
                if parts[0] not in new_scoring:
                    new_scoring[parts[0]] = {}
                new_scoring[parts[0]][parts[1]] = data
            else:
                new_scoring[path] = data

    for path, editor in editors:
        if path == "balcony_bonus._2k_fields" and isinstance(editor, NumberFields):
            data = editor.get_data()
            if "balcony_bonus" not in new_scoring:
                new_scoring["balcony_bonus"] = {}
            bb = new_scoring["balcony_bonus"]
            bb["two_k_bonus_ratio_range"] = [
                data.get("two_k_bonus_ratio_range_min", 1.5),
                data.get("two_k_bonus_ratio_range_max", 2),
            ]
            bb["two_k_bonus2_ratio_min"] = data.get("two_k_bonus2_ratio_min", 2)

    return new_scoring


def _save(scoring: dict, editors: list, full_cfg: dict):
    """Save all changes to eva.yaml."""
    errors = []
    for path, editor in editors:
        if isinstance(editor, ThresholdEditor):
            errs = editor.validate()
            if errs:
                errors.extend([f"{path}: {e}" for e in errs])

    if errors:
        ui.notify('\n'.join(errors), type='negative', multi_line=True, timeout=8000)
        return

    try:
        new_scoring = _collect_scoring(scoring, editors)
        save_scoring(new_scoring)

        for path, editor in editors:
            if path == "_complex_aliases" and isinstance(editor, MapEditor):
                ca_data = editor.get_data()
                ba_data = {}
                for p, ed in editors:
                    if p == "_building_aliases" and isinstance(ed, dict):
                        for cname, med in ed.items():
                            ba_data[cname] = med.get_data()
                save_aliases(ca_data, ba_data)
                break

        ui.notify('Сохранено', type='positive')
    except Exception as e:
        ui.notify(f'Ошибка сохранения: {e}', type='negative')


def _show_preview(scoring: dict, editors: list):
    """Show formula preview dialog."""
    try:
        new_scoring = _collect_scoring(scoring, editors)
        f1 = preview_formula(new_scoring, 1)
        f2 = preview_formula(new_scoring, 2)
    except Exception as e:
        ui.notify(f'Ошибка генерации формулы: {e}', type='negative')
        return

    with ui.dialog() as dialog, ui.card().classes('glass-card w-full max-w-4xl p-6'):
        ui.label('Предпросмотр формул').classes('text-heading')

        ui.label('Первый этап (row=3):').style(
            'font-size: 13px; color: var(--text-secondary); font-weight: 600; margin-top: 16px;'
        )
        ui.code(f1, language='excel').classes('w-full font-mono text-xs')

        ui.label('Второй этап (row=3):').style(
            'font-size: 13px; color: var(--text-secondary); font-weight: 600; margin-top: 16px;'
        )
        ui.code(f2, language='excel').classes('w-full font-mono text-xs')

        ui.button('Закрыть', on_click=dialog.close) \
            .props('flat no-caps').classes('self-end mt-4') \
            .style('color: var(--text-muted);')

    dialog.open()


def _reload_page():
    """Reload page."""
    ui.navigate.reload()
