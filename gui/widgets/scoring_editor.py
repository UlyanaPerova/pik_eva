"""
PIK EVA GUI — Scoring Editor widgets.

Threshold editor, map editor, and yaml round-trip helpers.
"""
from __future__ import annotations

from pathlib import Path

import yaml
from nicegui import ui

CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"


def load_scoring() -> dict:
    with open(CONFIGS_DIR / "eva.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f).get("scoring", {})


def load_full_config() -> dict:
    with open(CONFIGS_DIR / "eva.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_scoring(scoring: dict):
    """Round-trip safe: read full yaml, update ONLY scoring, write back."""
    path = CONFIGS_DIR / "eva.yaml"
    with open(path, "r", encoding="utf-8") as f:
        full = yaml.safe_load(f)
    full["scoring"] = scoring
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(full, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


def save_aliases(complex_aliases: dict, building_aliases: dict):
    path = CONFIGS_DIR / "eva.yaml"
    with open(path, "r", encoding="utf-8") as f:
        full = yaml.safe_load(f)
    full["complex_aliases"] = complex_aliases
    full["building_aliases"] = building_aliases
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(full, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


def preview_formula(scoring: dict, stage: int, row: int = 3) -> str:
    from core.scoring import generate_first_stage_formula, generate_second_stage_formula
    if stage == 1:
        return generate_first_stage_formula(row, scoring=scoring)
    return generate_second_stage_formula(row, scoring=scoring)


class ThresholdEditor:
    """Editor for [{max: N, points: P}, ...] lists with +/- buttons."""

    def __init__(self, thresholds: list[dict], on_change=None):
        self.thresholds = [dict(t) for t in thresholds]
        self._on_change = on_change
        self._container = None

    def build(self):
        self._container = ui.column().classes('gap-2 w-full')
        self._rebuild()

    def _rebuild(self):
        if not self._container:
            return
        self._container.clear()
        with self._container:
            for i, t in enumerate(self.thresholds):
                with ui.row().classes('items-center gap-2'):
                    ui.label('до').style('color: var(--text-muted); font-size: 12px;')
                    ui.number(value=t['max'], format='%.0f',
                              on_change=lambda e, idx=i: self._update_max(idx, e.value)) \
                        .style('width: 90px;').props('dense dark')
                    ui.label('\u2192').style('color: var(--text-muted);')
                    ui.number(value=t['points'], format='%.1f',
                              on_change=lambda e, idx=i: self._update_pts(idx, e.value)) \
                        .style('width: 70px;').props('dense dark')
                    ui.label('баллов').style('color: var(--text-muted); font-size: 12px;')
                    if len(self.thresholds) > 1:
                        ui.button(icon='close',
                                  on_click=lambda idx=i: self._remove(idx)) \
                            .props('flat round dense size=sm') \
                            .style('color: var(--error);')
            ui.button('+ Добавить порог', on_click=self._add) \
                .props('flat no-caps size=sm') \
                .style('color: var(--primary);')

    def _update_max(self, idx, val):
        if val is not None:
            self.thresholds[idx]['max'] = val
        if self._on_change:
            self._on_change()

    def _update_pts(self, idx, val):
        if val is not None:
            self.thresholds[idx]['points'] = val
        if self._on_change:
            self._on_change()

    def _remove(self, idx):
        if len(self.thresholds) > 1:
            self.thresholds.pop(idx)
            self._rebuild()
            if self._on_change:
                self._on_change()

    def _add(self):
        last = self.thresholds[-1] if self.thresholds else {'max': 100, 'points': 0}
        self.thresholds.append({'max': last['max'] + 10, 'points': 0})
        self._rebuild()
        if self._on_change:
            self._on_change()

    def get_data(self) -> list[dict]:
        return self.thresholds

    def validate(self) -> list[str]:
        errors = []
        for i in range(1, len(self.thresholds)):
            if self.thresholds[i]['max'] <= self.thresholds[i-1]['max']:
                errors.append(
                    f"Порог {i+1} ({self.thresholds[i]['max']}) "
                    f"должен быть больше порога {i} ({self.thresholds[i-1]['max']})"
                )
        return errors


class MapEditor:
    """Editor for {label: points} dictionaries with +/- buttons."""

    def __init__(self, mapping: dict, on_change=None):
        self.items = list(mapping.items())
        self._on_change = on_change
        self._container = None

    def build(self):
        self._container = ui.column().classes('gap-2 w-full')
        self._rebuild()

    def _rebuild(self):
        if not self._container:
            return
        self._container.clear()
        with self._container:
            for i, (label, pts) in enumerate(self.items):
                with ui.row().classes('items-center gap-2'):
                    ui.input(value=label,
                             on_change=lambda e, idx=i: self._update_label(idx, e.value)) \
                        .style('width: 140px;').props('dense dark')
                    ui.label('\u2192').style('color: var(--text-muted);')
                    ui.number(value=pts, format='%.1f',
                              on_change=lambda e, idx=i: self._update_pts(idx, e.value)) \
                        .style('width: 70px;').props('dense dark')
                    ui.label('баллов').style('color: var(--text-muted); font-size: 12px;')
                    if len(self.items) > 1:
                        ui.button(icon='close',
                                  on_click=lambda idx=i: self._remove(idx)) \
                            .props('flat round dense size=sm') \
                            .style('color: var(--error);')
            ui.button('+ Добавить вариант', on_click=self._add) \
                .props('flat no-caps size=sm') \
                .style('color: var(--primary);')

    def _update_label(self, idx, val):
        old_label, pts = self.items[idx]
        self.items[idx] = (val or old_label, pts)
        if self._on_change:
            self._on_change()

    def _update_pts(self, idx, val):
        label, _ = self.items[idx]
        self.items[idx] = (label, val if val is not None else 0)
        if self._on_change:
            self._on_change()

    def _remove(self, idx):
        if len(self.items) > 1:
            self.items.pop(idx)
            self._rebuild()
            if self._on_change:
                self._on_change()

    def _add(self):
        self.items.append(("Новый", 0))
        self._rebuild()
        if self._on_change:
            self._on_change()

    def get_data(self) -> dict:
        return dict(self.items)


class NumberFields:
    """Editor for a set of named numeric fields."""

    def __init__(self, fields: dict[str, tuple[str, float]], on_change=None):
        self.fields = fields
        self.values = {k: v for k, (_, v) in fields.items()}
        self._on_change = on_change

    def build(self):
        with ui.column().classes('gap-3 w-full'):
            for key, (label, val) in self.fields.items():
                with ui.row().classes('items-center gap-3'):
                    ui.label(label).style(
                        'font-size: 13px; color: var(--text-secondary); width: 200px;'
                    )
                    ui.number(value=val,
                              on_change=lambda e, k=key: self._update(k, e.value)) \
                        .style('width: 120px;').props('dense dark')

    def _update(self, key, val):
        if val is not None:
            self.values[key] = val
        if self._on_change:
            self._on_change()

    def get_data(self) -> dict:
        return dict(self.values)
