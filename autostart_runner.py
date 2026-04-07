#!/usr/bin/env python3
"""
PIK EVA — Автоматический парсинг при старте Windows.

Запускает парсинг для настроенных застройщиков.
Вызывается из .bat файла в автозагрузке.
"""
import asyncio
import json
import sys
from datetime import datetime, date
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))

from orchestrator import TaskRunner, RUNNER_SCRIPTS

LOCK_FILE = PROJECT_DIR / "data" / "autoparse_last_run.json"


def _already_ran_today() -> bool:
    """Проверить, запускался ли парсинг сегодня."""
    if not LOCK_FILE.exists():
        return False
    try:
        with open(LOCK_FILE, "r") as f:
            data = json.load(f)
        return data.get("date") == str(date.today())
    except Exception:
        return False


def _mark_ran():
    """Отметить, что парсинг запущен сегодня."""
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOCK_FILE, "w") as f:
        json.dump({"date": str(date.today()), "time": datetime.now().isoformat()}, f)


async def main():
    # Проверка: уже запускался сегодня?
    if _already_ran_today():
        print(f"[{datetime.now():%H:%M}] Парсинг уже выполнялся сегодня, пропуск.")
        return

    # Загрузить конфиг
    from autostart import get_config
    cfg = get_config()
    developers = cfg.get("developers", ["pik", "glorax", "smu88", "akbarsdom"])
    types = cfg.get("types", ["store", "apt"])

    # Собрать задачи
    tasks = []
    for typ in types:
        for dev in developers:
            script = RUNNER_SCRIPTS.get((typ, dev))
            if script:
                tasks.append((typ, dev, script))

    if not tasks:
        print("Нет задач для автопарсинга.")
        return

    print(f"[{datetime.now():%H:%M}] Автопарсинг: {len(tasks)} задач")
    print(f"  Застройщики: {', '.join(developers)}")
    print(f"  Типы: {', '.join(types)}")
    print()

    # Запуск
    async def log_cb(text, tag):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"  [{ts}] {text}")

    async def status_cb(task_id, status):
        pass

    runner = TaskRunner(log_callback=log_cb, status_callback=status_cb)
    await runner.run_tasks(tasks)

    _mark_ran()
    print(f"\n[{datetime.now():%H:%M}] Автопарсинг завершён.")


if __name__ == "__main__":
    asyncio.run(main())
