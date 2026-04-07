#!/usr/bin/env python3
"""
PIK EVA — Оркестрант.

Ядро оркестрации: запуск парсеров, определения застройщиков,
маппинг скриптов. Не зависит от GUI.

Использование напрямую (без GUI):
    from orchestrator import TaskRunner, DEVELOPERS, RUNNER_SCRIPTS

Запуск с визуализацией:
    python server.py → http://localhost:8080
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Callable, Awaitable

PROJECT_DIR = Path(__file__).resolve().parent

if sys.platform == "win32":
    VENV_PYTHON = str(PROJECT_DIR / ".venv" / "Scripts" / "python.exe")
else:
    VENV_PYTHON = str(PROJECT_DIR / ".venv" / "bin" / "python")

# ── Застройщики ──

DEVELOPERS = [
    ("pik",       "ПИК"),
    ("glorax",    "GloraX"),
    ("smu88",     "СМУ-88"),
    ("akbarsdom", "Ак Барс Дом"),
    ("unistroy",  "УниСтрой"),
]

DEV_LABELS = dict(DEVELOPERS + [("domrf", "ДОМ.РФ"), ("eva", "EVA")])
TYPE_LABELS = {"store": "кладовки", "apt": "квартиры", "eva": "EVA"}

# ── Маппинг скриптов ──

RUNNER_SCRIPTS = {
    ("store", "pik"):       "runners/run_storehouses/run_pik.py",
    ("store", "glorax"):    "runners/run_storehouses/run_glorax.py",
    ("store", "smu88"):     "runners/run_storehouses/run_smu88.py",
    ("store", "akbarsdom"): "runners/run_storehouses/run_akbarsdom.py",
    ("store", "unistroy"):  "runners/run_storehouses/run_unistroy.py",
    ("store", "domrf"):     "runners/run_storehouses/run_domrf.py",
    ("apt",   "pik"):       "runners/run_apartments/run_pik.py",
    ("apt",   "glorax"):    "runners/run_apartments/run_glorax.py",
    ("apt",   "smu88"):     "runners/run_apartments/run_smu88.py",
    ("apt",   "akbarsdom"): "runners/run_apartments/run_akbarsdom.py",
    ("apt",   "unistroy"):  "runners/run_apartments/run_unistroy.py",
    ("apt",   "domrf"):     "runners/run_apartments/run_domrf.py",
    ("eva",   "eva"):       "runners/run_eva.py",
}

# ── Типы колбэков ──

LogCallback = Callable[[str, str], Awaitable[None]]
StatusCallback = Callable[[str, str], Awaitable[None]]


# ── TaskRunner ──

class TaskRunner:
    """Async task runner for parser scripts.

    Запускает скрипты парсеров через asyncio subprocess, последовательно.
    Обратная связь через async-колбэки log_callback и status_callback.
    """

    def __init__(self, log_callback: LogCallback, status_callback: StatusCallback):
        self._running = False
        self._log = log_callback       # async (text, tag) -> consumer
        self._status = status_callback  # async (task_id, status) -> consumer

    @property
    def is_running(self) -> bool:
        return self._running

    async def run_tasks(
        self,
        tasks: list[tuple[str, str, str]],
        on_complete: Callable[[], Awaitable[None]] | None = None,
    ):
        """Run a list of (type, key, script) tasks sequentially."""
        if self._running:
            await self._log("Уже выполняется, подождите...", "fail")
            return
        if not tasks:
            await self._log("Ничего не выбрано", "info")
            return

        self._running = True
        try:
            total = len(tasks)
            for i, (typ, key, script) in enumerate(tasks, 1):
                label = f"{DEV_LABELS.get(key, key)} {TYPE_LABELS.get(typ, typ)}"
                await self._log(f"[{i}/{total}] Запуск: {label}...", "info")
                await self._status(f"{typ}:{key}", "running")

                try:
                    proc = await asyncio.create_subprocess_exec(
                        VENV_PYTHON, str(PROJECT_DIR / script),
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        cwd=str(PROJECT_DIR),
                    )
                    stdout, stderr = await asyncio.wait_for(
                        proc.communicate(), timeout=600,
                    )

                    if proc.returncode == 0:
                        lines = (stdout or b"").decode().strip().split("\n")
                        summary = ""
                        for line in reversed(lines):
                            if any(kw in line for kw in ("Всего", "Файл готов", "Готово")):
                                summary = line.strip()
                                break
                        msg = f"[OK] {label}"
                        if summary:
                            msg += f" -- {summary}"
                        await self._log(msg, "ok")
                        await self._status(f"{typ}:{key}", "ok")
                    else:
                        err_lines = (stderr or b"").decode().strip().split("\n")
                        err = err_lines[-1] if err_lines else "неизвестная ошибка"
                        await self._log(f"[FAIL] {label}: {err}", "fail")
                        await self._status(f"{typ}:{key}", "fail")

                except asyncio.TimeoutError:
                    await self._log(f"[TIMEOUT] {label}: превышено 10 минут", "fail")
                    await self._status(f"{typ}:{key}", "fail")
                except Exception as e:
                    await self._log(f"[ERROR] {label}: {e}", "fail")
                    await self._status(f"{typ}:{key}", "fail")

            await self._log("Выполнение завершено.", "info")
        finally:
            self._running = False
            if on_complete:
                await on_complete()
