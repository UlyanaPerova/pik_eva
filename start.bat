@echo off
chcp 65001 >nul 2>&1
title PIK EVA
cd /d "%~dp0"

echo.
echo   ╔══════════════════════════════════╗
echo   ║         PIK EVA  v1.0           ║
echo   ║    Оркестрант недвижимости       ║
echo   ╚══════════════════════════════════╝
echo.

rem ── Проверка Python ──
python --version >nul 2>&1
if errorlevel 1 (
    echo   [!] Python не найден.
    echo       Скачайте: https://python.org/downloads
    echo       При установке отметьте "Add Python to PATH"
    echo.
    pause
    exit /b 1
)

rem ── Проверка Git ──
git --version >nul 2>&1
if errorlevel 1 (
    echo   [!] Git не найден.
    echo       Скачайте: https://git-scm.com/download/win
    echo       Обновления через GUI будут недоступны.
    echo.
)

rem ── Создание venv при первом запуске ──
if not exist ".venv" (
    echo   [*] Первый запуск — настройка окружения...
    echo       Это займет 1-2 минуты.
    echo.
    python -m venv .venv
    if errorlevel 1 (
        echo   [!] Ошибка создания виртуального окружения.
        pause
        exit /b 1
    )
    echo   [*] Установка зависимостей...
    .venv\Scripts\pip install -r requirements.txt -q
    echo   [*] Установка браузера для парсинга...
    .venv\Scripts\python -m playwright install chromium
    echo.
    echo   [OK] Настройка завершена!
    echo.
)

rem ── Тихое обновление зависимостей (после git pull) ──
.venv\Scripts\pip install -r requirements.txt -q 2>nul

rem ── Проверка браузера Playwright ──
if not exist "%LOCALAPPDATA%\ms-playwright\chromium*" (
    echo   [*] Установка браузера для парсинга...
    .venv\Scripts\python -m playwright install chromium
)

rem ── Проверка: не запущен ли уже сервер ──
netstat -ano | findstr ":8090" >nul 2>&1
if not errorlevel 1 (
    echo   [*] Сервер уже запущен — открываю браузер...
    start http://localhost:8090
    timeout /t 1 /nobreak >nul
    exit /b 0
)

rem ── Запуск сервера ──
echo   [*] Запуск PIK EVA...
start /min "PIK EVA Server" .venv\Scripts\python site\api.py

rem ── Ожидание готовности сервера ──
echo   [*] Ожидание сервера...
set /a tries=0
:wait_loop
timeout /t 1 /nobreak >nul
set /a tries+=1
curl -s http://localhost:8090/api/status >nul 2>&1
if not errorlevel 1 goto server_ready
if %tries% lss 15 goto wait_loop

echo   [!] Сервер не запустился за 15 секунд.
pause
exit /b 1

:server_ready
echo   [OK] Сервер готов!
echo.
echo   Открываю http://localhost:8090 ...
echo   (это окно можно закрыть)
echo.
start http://localhost:8090
