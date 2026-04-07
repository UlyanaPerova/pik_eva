"""
PIK EVA GUI — Toast notifications wrapper.
"""
from nicegui import ui


def toast_ok(msg: str):
    ui.notify(msg, type='positive', position='top-right', timeout=3000)


def toast_fail(msg: str):
    ui.notify(msg, type='negative', position='top-right', timeout=5000)


def toast_warn(msg: str):
    ui.notify(msg, type='warning', position='top-right', timeout=4000)


def toast_info(msg: str):
    ui.notify(msg, type='info', position='top-right', timeout=3000)
