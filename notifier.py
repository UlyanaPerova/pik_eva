#!/usr/bin/env python3
"""
PIK EVA — Telegram-уведомления.

Отправляет ошибки и результаты парсинга в Telegram.
Конфигурация хранится в notifier_config.json (gitignored).
"""
from __future__ import annotations

import json
import logging
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("notifier")

CONFIG_PATH = Path(__file__).resolve().parent / "notifier_config.json"

_DEFAULT_CONFIG = {
    "enabled": True,
    "bot_token": "8550486622:AAH-7jgZhaJ3cxWWiE9TuNi4MXKWyLu3C8U",
    "chat_id": "596836149",
    "send_errors": True,
    "send_summary": True,
}


def _load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    # Создаём конфиг по умолчанию
    _save_config(_DEFAULT_CONFIG)
    return _DEFAULT_CONFIG.copy()


def _save_config(cfg: dict) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def send_telegram(text: str) -> bool:
    """Отправить сообщение в Telegram. Возвращает True при успехе."""
    cfg = _load_config()
    if not cfg.get("enabled"):
        return False

    token = cfg.get("bot_token", "")
    chat_id = cfg.get("chat_id", "")
    if not token or not chat_id:
        return False

    # Ограничиваем длину (Telegram max 4096)
    if len(text) > 4000:
        text = text[:4000] + "\n... (обрезано)"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        logger.debug("Telegram send failed: %s", e)
        return False


def _version_line(version: str) -> str:
    """Хэш коммита + текущее время."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S %z").strip()
    return f"{version} {now}"


def notify_error(version: str, label: str, error: str) -> None:
    """Отправить уведомление об ошибке."""
    cfg = _load_config()
    if not cfg.get("send_errors"):
        return
    text = (
        f"<b>PIK EVA — Ошибка</b>\n"
        f"<code>{_version_line(version)}</code>\n\n"
        f"<b>{label}</b>\n"
        f"{_escape(error)}"
    )
    send_telegram(text)


def notify_summary(version: str, results: list[tuple[str, str]]) -> None:
    """Отправить итог парсинга. results = [(label, status), ...]"""
    cfg = _load_config()
    if not cfg.get("send_summary"):
        return

    lines = [f"<b>PIK EVA — Итог</b>", f"<code>{_version_line(version)}</code>\n"]
    for label, status in results:
        icon = {"ok": "\u2705", "fail": "\u274c", "timeout": "\u23f0"}.get(status, "\u2753")
        lines.append(f"{icon} {label}")

    send_telegram("\n".join(lines))


def _escape(text: str) -> str:
    """Экранировать HTML-спецсимволы."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
