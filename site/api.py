#!/usr/bin/env python3
"""
PIK EVA — FastAPI backend для веб-интерфейса.

Запуск:
    cd site && python api.py
    → http://localhost:8090
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import AsyncGenerator

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Пути ──

SITE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SITE_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

from orchestrator import TaskRunner, DEVELOPERS, RUNNER_SCRIPTS, DEV_LABELS, TYPE_LABELS
from config_manager import (
    list_links, list_complexes, add_link, remove_link,
    sync_configs, get_status, get_last_run_info, get_scoring_config,
    CONFIGS_DIR,
)

import yaml
import autostart

APT_DB = PROJECT_DIR / "data" / "apartments" / "apartments_history.db"
STORE_DB = PROJECT_DIR / "data" / "history.db"

# ── App ──

app = FastAPI(title="PIK EVA API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Глобальное состояние задач ──

task_logs: list[dict] = []
task_running = False
task_statuses: dict[str, str] = {}


async def _log_cb(text: str, tag: str):
    task_logs.append({"text": text, "tag": tag})


async def _status_cb(task_id: str, status: str):
    task_statuses[task_id] = status


runner = TaskRunner(log_callback=_log_cb, status_callback=_status_cb)


# ══════════════════════════════════════
#  STATUS
# ══════════════════════════════════════

@app.get("/api/status")
def api_status():
    """Общий статус системы."""
    status = get_status()
    devs = []
    for key, label in DEVELOPERS:
        dev = {
            "key": key,
            "label": label,
            "storehouses": status["storehouses"]["sites"].get(key),
            "apartments": status["apartments"]["sites"].get(key),
        }
        devs.append(dev)
    # ДОМ.РФ отдельно
    domrf_store = status["storehouses"]["sites"].get("domrf")
    domrf_apt = status["apartments"]["sites"].get("domrf")
    devs.append({
        "key": "domrf",
        "label": "ДОМ.РФ",
        "storehouses": domrf_store,
        "apartments": domrf_apt,
    })
    return {
        "developers": devs,
        "config": status["config"],
        "db": {
            "apartments_exists": status["apartments"]["db_exists"],
            "storehouses_exists": status["storehouses"]["db_exists"],
        },
    }


@app.get("/api/status/{site}")
def api_site_status(site: str):
    """Детали по сайту."""
    return get_last_run_info(site)


# ══════════════════════════════════════
#  TASKS
# ══════════════════════════════════════

class TaskRequest(BaseModel):
    tasks: list[dict]  # [{type: "store"|"apt"|"eva", key: "pik"|"domrf"|...}]


@app.post("/api/tasks/run")
async def api_run_tasks(req: TaskRequest):
    """Запустить задачи парсинга."""
    global task_running
    if runner.is_running:
        raise HTTPException(409, "Задачи уже выполняются")

    task_logs.clear()
    task_statuses.clear()

    resolved = []
    for t in req.tasks:
        typ, key = t["type"], t["key"]
        script = RUNNER_SCRIPTS.get((typ, key))
        if script:
            resolved.append((typ, key, script))

    if not resolved:
        raise HTTPException(400, "Нет валидных задач")

    asyncio.create_task(runner.run_tasks(resolved))
    return {"status": "started", "count": len(resolved)}


@app.get("/api/tasks/stream")
async def api_task_stream():
    """SSE стрим логов выполнения."""
    async def generate() -> AsyncGenerator[str, None]:
        sent = 0
        while True:
            if sent < len(task_logs):
                for entry in task_logs[sent:]:
                    data = json.dumps(entry, ensure_ascii=False)
                    yield f"data: {data}\n\n"
                sent = len(task_logs)

            if not runner.is_running and sent >= len(task_logs):
                yield f"data: {json.dumps({'text': '__DONE__', 'tag': 'done'})}\n\n"
                break

            await asyncio.sleep(0.3)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/tasks/status")
def api_task_status():
    """Текущий статус задач."""
    return {
        "running": runner.is_running,
        "statuses": task_statuses,
        "log_count": len(task_logs),
    }


# ══════════════════════════════════════
#  LINKS (ДОМ.РФ)
# ══════════════════════════════════════

@app.get("/api/links")
def api_list_links():
    links = list_links()
    # Check EVA file for domrf counts per object_id
    eva_file = PROJECT_DIR / "расчет_ева.xlsx"
    eva_exists = eva_file.exists()
    # For now, use DB to check if domrf items exist per object_id
    domrf_apt_ids = set()
    domrf_store_ids = set()
    if APT_DB.exists():
        conn = sqlite3.connect(str(APT_DB))
        try:
            rows = conn.execute("SELECT DISTINCT object_id FROM apartment_prices WHERE site='domrf' AND object_id IS NOT NULL").fetchall()
            domrf_apt_ids = {r[0] for r in rows}
        finally:
            conn.close()
    if STORE_DB.exists():
        conn = sqlite3.connect(str(STORE_DB))
        try:
            rows = conn.execute("SELECT DISTINCT object_id FROM prices WHERE site='domrf' AND object_id IS NOT NULL").fetchall()
            domrf_store_ids = {r[0] for r in rows}
        finally:
            conn.close()
    for link in links:
        oid = link["object_id"]
        link["has_apt_data"] = oid in domrf_apt_ids
        link["has_store_data"] = oid in domrf_store_ids
    return {"links": links, "complexes": list_complexes(), "eva_exists": eva_exists}


class LinkRequest(BaseModel):
    object_id: int
    complex_name: str
    building: str = ""
    developer: str = ""
    city: str = "Казань"
    add_to_apartments: bool = True
    add_to_storehouses: bool = True


@app.post("/api/links")
def api_add_link(req: LinkRequest):
    try:
        result = add_link(
            object_id=req.object_id,
            complex_name=req.complex_name,
            building=req.building,
            developer=req.developer,
            city=req.city,
            add_to_apartments=req.add_to_apartments,
            add_to_storehouses=req.add_to_storehouses,
        )
        return {"result": result}
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.delete("/api/links/{object_id}")
def api_remove_link(object_id: int):
    result = remove_link(object_id)
    return {"result": result}


@app.post("/api/links/sync")
def api_sync_links():
    return {"result": sync_configs()}


# ══════════════════════════════════════
#  SCORING (EVA формулы)
# ══════════════════════════════════════

@app.get("/api/scoring")
def api_get_scoring():
    eva_path = CONFIGS_DIR / "eva.yaml"
    if not eva_path.exists():
        return {}
    with open(eva_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return {
        "scoring": cfg.get("scoring", {}),
        "complex_aliases": cfg.get("complex_aliases", {}),
        "building_aliases": cfg.get("building_aliases", {}),
    }


class ScoringUpdate(BaseModel):
    scoring: dict | None = None
    complex_aliases: dict | None = None
    building_aliases: dict | None = None


@app.put("/api/scoring")
def api_update_scoring(req: ScoringUpdate):
    eva_path = CONFIGS_DIR / "eva.yaml"
    with open(eva_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if req.scoring is not None:
        cfg["scoring"] = req.scoring
    if req.complex_aliases is not None:
        cfg["complex_aliases"] = req.complex_aliases
    if req.building_aliases is not None:
        cfg["building_aliases"] = req.building_aliases

    with open(eva_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False, sort_keys=False, width=120)

    return {"status": "saved"}


@app.post("/api/scoring/reset")
def api_reset_scoring():
    """Сбросить формулы до заводских (из git HEAD)."""
    eva_path = CONFIGS_DIR / "eva.yaml"
    try:
        # Restore eva.yaml from git
        result = subprocess.check_output(
            ["git", "checkout", "HEAD", "--", str(eva_path)],
            cwd=str(PROJECT_DIR), text=True, stderr=subprocess.STDOUT,
        )
        return {"status": "reset"}
    except subprocess.CalledProcessError as e:
        raise HTTPException(500, f"Ошибка сброса: {e.output.strip()}")


# ══════════════════════════════════════
#  EVA GENERATE
# ══════════════════════════════════════

@app.post("/api/eva/generate")
async def api_generate_eva():
    """Запустить генерацию расчет_ева.xlsx."""
    if runner.is_running:
        raise HTTPException(409, "Задачи уже выполняются")

    task_logs.clear()
    task_statuses.clear()
    asyncio.create_task(runner.run_tasks([("eva", "eva", "runners/run_eva.py")]))
    return {"status": "started"}


# ══════════════════════════════════════
#  GIT
# ══════════════════════════════════════

@app.get("/api/git/status")
def api_git_status():
    try:
        branch = subprocess.check_output(
            ["git", "branch", "--show-current"],
            cwd=str(PROJECT_DIR), text=True,
        ).strip()
        commit = subprocess.check_output(
            ["git", "log", "-1", "--format=%h %s"],
            cwd=str(PROJECT_DIR), text=True,
        ).strip()
        # Check if behind remote
        subprocess.run(
            ["git", "fetch", "--dry-run"],
            cwd=str(PROJECT_DIR), capture_output=True, timeout=10,
        )
        behind = subprocess.check_output(
            ["git", "rev-list", "--count", f"{branch}..origin/{branch}"],
            cwd=str(PROJECT_DIR), text=True,
        ).strip()
        return {
            "branch": branch,
            "commit": commit,
            "behind": int(behind) if behind.isdigit() else 0,
        }
    except Exception as e:
        return {"branch": "unknown", "commit": str(e), "behind": 0}


@app.post("/api/git/pull")
def api_git_pull():
    try:
        result = subprocess.check_output(
            ["git", "pull", "--ff-only"],
            cwd=str(PROJECT_DIR), text=True, stderr=subprocess.STDOUT,
        )
        return {"status": "ok", "output": result.strip()}
    except subprocess.CalledProcessError as e:
        raise HTTPException(500, e.output.strip())


# ══════════════════════════════════════
#  LOGS
# ══════════════════════════════════════

@app.get("/api/logs")
def api_logs():
    """Получить последние логи из файлов в logs/."""
    logs_dir = PROJECT_DIR / "logs"
    lines = []
    if logs_dir.exists():
        log_files = sorted(logs_dir.glob("*.log"), key=lambda f: f.stat().st_mtime, reverse=True)
        for lf in log_files[:3]:  # последние 3 файла
            try:
                content = lf.read_text(encoding="utf-8", errors="replace")
                file_lines = content.strip().split("\n")
                # Берём последние 100 строк из каждого файла
                lines.extend(file_lines[-100:])
            except Exception:
                pass
    # Убираем пустые строки и технический мусор
    clean = []
    for line in lines[-200:]:
        line = line.strip()
        if not line:
            continue
        # Упрощаем вывод
        line = line.replace("INFO:root:", "").replace("WARNING:root:", "Предупреждение: ")
        line = line.replace("ERROR:root:", "Ошибка: ")
        clean.append(line)
    return {"logs": clean}


# ══════════════════════════════════════
#  AUTOSTART
# ══════════════════════════════════════

@app.get("/api/autostart")
def api_autostart_status():
    return autostart.get_status()


class AutostartConfig(BaseModel):
    enabled: bool | None = None
    run_after_hour: int | None = None
    developers: list[str] | None = None
    types: list[str] | None = None


@app.post("/api/autostart/toggle")
def api_autostart_toggle(req: AutostartConfig):
    cfg = autostart.get_config()
    if req.run_after_hour is not None:
        cfg["run_after_hour"] = req.run_after_hour
    if req.developers is not None:
        cfg["developers"] = req.developers
    if req.types is not None:
        cfg["types"] = req.types

    cfg["enabled"] = bool(req.enabled)
    autostart.save_config(cfg)

    if autostart.is_supported():
        if req.enabled:
            autostart.enable(cfg)
        else:
            autostart.disable()

    return autostart.get_status()


# ══════════════════════════════════════
#  STATIC FILES + RUN
# ══════════════════════════════════════

# Serve photos from photos_links directory
photos_dir = PROJECT_DIR / "photos_links"
if photos_dir.exists():
    app.mount("/photos", StaticFiles(directory=str(photos_dir)), name="photos")

app.mount("/", StaticFiles(directory=str(SITE_DIR), html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8090, log_level="info")
