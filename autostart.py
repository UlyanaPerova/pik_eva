"""
PIK EVA — Модуль автозапуска парсинга для Windows.

Регистрирует автоматический парсинг при старте Windows.
Запускает сбор данных (кладовки + квартиры) для всех застройщиков,
кроме ДОМ.РФ и Унистрой (требуют капчу/CDP).

Условия запуска:
- После 11:00 (один раз в день)
- Наличие подключения к интернету
- Автозапуск при старте Windows

Использует папку Startup (не требует прав администратора).
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
BAT_NAME = "pik_eva_autoparse.bat"
CONFIG_NAME = "autostart_config.json"

# Застройщики, которые парсятся автоматически (без капчи)
AUTO_DEVELOPERS = ["pik", "glorax", "smu88", "akbarsdom"]
# Исключены: domrf (капча), unistroy (CDP не подключён)

DEFAULT_CONFIG = {
    "enabled": False,
    "run_after_hour": 11,       # запускать после 11:00
    "developers": AUTO_DEVELOPERS,
    "types": ["store", "apt"],  # кладовки и квартиры
}


def _startup_dir() -> Path | None:
    """Путь к папке автозагрузки Windows."""
    if sys.platform != "win32":
        return None
    appdata = os.environ.get("APPDATA")
    if not appdata:
        return None
    startup = Path(appdata) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
    return startup if startup.exists() else None


def _bat_path() -> Path | None:
    d = _startup_dir()
    return d / BAT_NAME if d else None


def _config_path() -> Path:
    return PROJECT_DIR / CONFIG_NAME


def _load_config() -> dict:
    p = _config_path()
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            return {**DEFAULT_CONFIG, **json.load(f)}
    return dict(DEFAULT_CONFIG)


def _save_config(cfg: dict) -> None:
    with open(_config_path(), "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def _generate_bat_content(cfg: dict) -> str:
    """Сгенерировать .bat с проверкой времени и интернета."""
    venv_python = PROJECT_DIR / ".venv" / "Scripts" / "python.exe"
    runner_script = PROJECT_DIR / "autostart_runner.py"
    hour = cfg.get("run_after_hour", 11)

    return (
        f"@echo off\n"
        f"rem PIK EVA — автозапуск парсинга\n"
        f"rem Проверка времени (после {hour}:00)\n"
        f"for /f \"tokens=1 delims=:\" %%h in (\"%time%\") do set /a hour=%%h\n"
        f"if %hour% LSS {hour} (\n"
        f"  echo Ещё рано, ждём {hour}:00. Запуск через планировщик.\n"
        f"  timeout /t 5\n"
        f"  exit /b 0\n"
        f")\n"
        f"\n"
        f"rem Проверка интернета\n"
        f"ping -n 1 8.8.8.8 >nul 2>&1\n"
        f"if errorlevel 1 (\n"
        f"  echo Нет подключения к интернету, пропускаем парсинг.\n"
        f"  timeout /t 5\n"
        f"  exit /b 0\n"
        f")\n"
        f"\n"
        f"echo Запуск автопарсинга PIK EVA...\n"
        f"cd /d \"{PROJECT_DIR}\"\n"
        f"\"{venv_python}\" \"{runner_script}\"\n"
    )


def is_supported() -> bool:
    return sys.platform == "win32" and _startup_dir() is not None


def is_enabled() -> bool:
    bat = _bat_path()
    return bat is not None and bat.exists()


def enable(cfg: dict | None = None) -> bool:
    """Включить автозапуск парсинга."""
    bat = _bat_path()
    if bat is None:
        return False
    config = _load_config()
    if cfg:
        config.update(cfg)
    config["enabled"] = True
    _save_config(config)
    bat.write_text(_generate_bat_content(config), encoding="utf-8")
    return True


def disable() -> bool:
    """Выключить автозапуск парсинга."""
    bat = _bat_path()
    if bat is None:
        return False
    if bat.exists():
        bat.unlink()
    config = _load_config()
    config["enabled"] = False
    _save_config(config)
    return True


def get_config() -> dict:
    return _load_config()


def save_config(cfg: dict) -> None:
    _save_config(cfg)
    if cfg.get("enabled") and is_supported():
        enable(cfg)


def get_status() -> dict:
    cfg = _load_config()
    return {
        "supported": is_supported(),
        "enabled": is_enabled(),
        "platform": sys.platform,
        "config": cfg,
    }
