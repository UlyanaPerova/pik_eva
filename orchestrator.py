#!/usr/bin/env python3
"""
PIK EVA — Оркестрант.

Ядро оркестрации: запуск парсеров, определения застройщиков,
маппинг скриптов. Не зависит от GUI.

Режимы запуска:
- Последовательный: задачи выполняются одна за другой
- Параллельный: застройщики параллельно, типы (кладовки/квартиры)
  внутри одного застройщика — последовательно (безопасный доступ к SQLite)

Использование напрямую (без GUI):
    from orchestrator import TaskRunner, DEVELOPERS, RUNNER_SCRIPTS

Запуск с визуализацией:
    python site/api.py → http://localhost:8090
"""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Callable, Awaitable

PROJECT_DIR = Path(__file__).resolve().parent


def get_version() -> str:
    """Получить короткий хэш и дату последнего коммита."""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%h %ci"],
            cwd=str(PROJECT_DIR),
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"

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


# ── Telegram-уведомления (fire-and-forget) ──

def _notify_error(version: str, label: str, error: str) -> None:
    try:
        from notifier import notify_error
        notify_error(version, label, error)
    except Exception:
        pass


def _notify_summary(version: str, results: list[tuple[str, str]]) -> None:
    try:
        from notifier import notify_summary
        notify_summary(version, results)
    except Exception:
        pass


# ── TaskRunner ──

class TaskRunner:
    """Async task runner for parser scripts.

    Параллельный режим (по умолчанию):
    - Застройщики запускаются параллельно
    - Кладовки и квартиры одного застройщика — последовательно
    - Это безопасно для SQLite (каждый застройщик пишет свои записи)

    Последовательный режим (parallel=False):
    - Все задачи выполняются одна за другой
    """

    def __init__(self, log_callback: LogCallback, status_callback: StatusCallback):
        self._running = False
        self._log = log_callback       # async (text, tag) -> consumer
        self._status = status_callback  # async (task_id, status) -> consumer
        self._results: list[tuple[str, str]] = []  # (label, status) для итога
        self._version = ""

    @property
    def is_running(self) -> bool:
        return self._running

    async def _run_one(self, typ: str, key: str, script: str) -> bool:
        """Запустить один скрипт. Возвращает True при успехе."""
        label = f"{DEV_LABELS.get(key, key)} {TYPE_LABELS.get(typ, typ)}"
        await self._log(f"Запуск: {label}...", "info")
        await self._status(f"{typ}:{key}", "running")

        try:
            env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
            proc = await asyncio.create_subprocess_exec(
                VENV_PYTHON, str(PROJECT_DIR / script),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(PROJECT_DIR),
                env=env,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=600,
            )

            if proc.returncode == 0:
                lines = (stdout or b"").decode("utf-8", errors="replace").strip().split("\n")
                summary = ""
                for line in reversed(lines):
                    if any(kw in line for kw in ("Всего", "Файл готов", "Готово")):
                        summary = line.strip()
                        break
                msg = f"[OK] {label}"
                if summary:
                    msg += f" — {summary}"
                await self._log(msg, "ok")
                await self._status(f"{typ}:{key}", "ok")
                self._results.append((label, "ok"))
                return True
            else:
                err_lines = (stderr or b"").decode("utf-8", errors="replace").strip().split("\n")
                err = err_lines[-1] if err_lines else "неизвестная ошибка"
                await self._log(f"[FAIL] {label}: {err}", "fail")
                await self._status(f"{typ}:{key}", "fail")
                self._results.append((label, "fail"))
                _notify_error(self._version, label, err)
                return False

        except asyncio.TimeoutError:
            await self._log(f"[TIMEOUT] {label}: превышено 10 минут", "fail")
            await self._status(f"{typ}:{key}", "fail")
            self._results.append((label, "timeout"))
            _notify_error(self._version, label, "Превышено 10 минут")
            return False
        except Exception as e:
            await self._log(f"[ERROR] {label}: {e}", "fail")
            await self._status(f"{typ}:{key}", "fail")
            self._results.append((label, "fail"))
            _notify_error(self._version, label, str(e))
            return False

    async def _run_developer(self, key: str, dev_tasks: list[tuple[str, str, str]]):
        """Запустить все задачи одного застройщика последовательно."""
        for typ, k, script in dev_tasks:
            await self._run_one(typ, k, script)

    async def run_tasks(
        self,
        tasks: list[tuple[str, str, str]],
        on_complete: Callable[[], Awaitable[None]] | None = None,
        parallel: bool = True,
    ):
        """Запустить задачи.

        Args:
            tasks: список (type, key, script)
            on_complete: колбэк после завершения всех задач
            parallel: True — застройщики параллельно, False — всё последовательно
        """
        if self._running:
            await self._log("Уже выполняется, подождите...", "fail")
            return
        if not tasks:
            await self._log("Ничего не выбрано", "info")
            return

        self._running = True
        self._version = get_version()
        self._results = []
        await self._log(f"Версия: {self._version}", "info")
        try:
            if parallel and len(tasks) > 1:
                # Группируем задачи по застройщику
                by_dev: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
                for typ, key, script in tasks:
                    by_dev[key].append((typ, key, script))

                dev_count = len(by_dev)
                task_count = len(tasks)
                await self._log(
                    f"Параллельный запуск: {task_count} задач, "
                    f"{dev_count} застройщиков одновременно",
                    "info",
                )

                # Запускаем застройщиков параллельно
                await asyncio.gather(
                    *(self._run_developer(key, dev_tasks)
                      for key, dev_tasks in by_dev.items())
                )
            else:
                # Последовательный режим
                total = len(tasks)
                for i, (typ, key, script) in enumerate(tasks, 1):
                    await self._log(f"[{i}/{total}]", "info")
                    await self._run_one(typ, key, script)

            await self._log("Выполнение завершено.", "info")
            _notify_summary(self._version, self._results)
        finally:
            self._running = False
            if on_complete:
                await on_complete()
