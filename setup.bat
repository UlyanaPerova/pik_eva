@echo off
chcp 65001 >nul 2>&1
title PIK EVA — Установка
cd /d "%~dp0"

echo.
echo   ╔══════════════════════════════════╗
echo   ║     PIK EVA — Установка         ║
echo   ╚══════════════════════════════════╝
echo.

rem ── Проверка Python ──
echo   [1/5] Проверка Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo         [!] Python не найден!
    echo         Скачайте: https://python.org/downloads
    echo         ВАЖНО: при установке отметьте "Add Python to PATH"
    echo.
    pause
    exit /b 1
)
for /f "tokens=2" %%v in ('python --version 2^>^&1') do echo         Python %%v — OK

rem ── Проверка Git ──
echo   [2/5] Проверка Git...
git --version >nul 2>&1
if errorlevel 1 (
    echo         [!] Git не найден!
    echo         Скачайте: https://git-scm.com/download/win
    echo         Без Git обновления через GUI будут недоступны.
    echo.
    set /p cont="         Продолжить без Git? (y/n): "
    if /i not "%cont%"=="y" exit /b 1
) else (
    for /f "tokens=3" %%v in ('git --version') do echo         Git %%v — OK
)

rem ── Создание виртуального окружения ──
echo   [3/5] Создание виртуального окружения...
if exist ".venv" (
    echo         .venv уже существует — пропускаю
) else (
    python -m venv .venv
    if errorlevel 1 (
        echo         [!] Ошибка создания .venv
        pause
        exit /b 1
    )
    echo         OK
)

rem ── Установка зависимостей ──
echo   [4/5] Установка зависимостей...
.venv\Scripts\pip install -r requirements.txt -q
if errorlevel 1 (
    echo         [!] Ошибка установки зависимостей
    pause
    exit /b 1
)
echo         OK

rem ── Playwright ──
echo   [5/5] Установка браузера для парсинга...
.venv\Scripts\python -m playwright install chromium
echo         OK

rem ── Ярлык на рабочем столе ──
echo.
echo   Создаю ярлык на рабочем столе...

set SCRIPT_PATH=%~dp0start.bat
set ICON_PATH=%~dp0icon.ico
set SHORTCUT_PATH=%USERPROFILE%\Desktop\PIK EVA.lnk

powershell -Command "$ws = New-Object -ComObject WScript.Shell; $sc = $ws.CreateShortcut('%SHORTCUT_PATH%'); $sc.TargetPath = '%SCRIPT_PATH%'; $sc.WorkingDirectory = '%~dp0'; $sc.IconLocation = '%ICON_PATH%'; $sc.Description = 'PIK EVA — Оркестрант недвижимости'; $sc.Save()"

if exist "%SHORTCUT_PATH%" (
    echo         Ярлык создан: %SHORTCUT_PATH%
) else (
    echo         [!] Не удалось создать ярлык
)

echo.
echo   ╔══════════════════════════════════╗
echo   ║     Установка завершена!         ║
echo   ║                                  ║
echo   ║  Запуск: двойной клик по         ║
echo   ║  иконке PIK EVA на рабочем      ║
echo   ║  столе или файлу start.bat       ║
echo   ╚══════════════════════════════════╝
echo.
pause
