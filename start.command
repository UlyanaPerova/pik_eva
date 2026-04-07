#!/bin/bash
# PIK EVA — Лаунчер для macOS
# Двойной клик в Finder → сервер + браузер

cd "$(dirname "$0")"

echo ""
echo "  ╔══════════════════════════════════╗"
echo "  ║         PIK EVA  v1.0           ║"
echo "  ║    Оркестрант недвижимости       ║"
echo "  ╚══════════════════════════════════╝"
echo ""

# ── Проверка Python ──
if ! command -v python3 &> /dev/null; then
    echo "  [!] Python3 не найден."
    echo "      Установите: brew install python3"
    echo "      Или скачайте: https://python.org/downloads"
    read -p "  Нажмите Enter для выхода..."
    exit 1
fi

# ── Создание venv при первом запуске ──
if [ ! -d ".venv" ]; then
    echo "  [*] Первый запуск — настройка окружения..."
    echo "      Это займет 1-2 минуты."
    echo ""
    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt -q
    .venv/bin/python -m playwright install chromium
    echo ""
    echo "  [OK] Настройка завершена!"
    echo ""
fi

# ── Тихое обновление зависимостей ──
.venv/bin/pip install -r requirements.txt -q 2>/dev/null

# ── Проверка: не запущен ли уже ──
if lsof -i :8090 &> /dev/null; then
    echo "  [*] Сервер уже запущен — открываю браузер..."
    open http://localhost:8090
    exit 0
fi

# ── Запуск сервера ──
echo "  [*] Запуск PIK EVA..."
.venv/bin/python site/api.py &
SERVER_PID=$!

# ── Ожидание готовности ──
echo "  [*] Ожидание сервера..."
for i in $(seq 1 15); do
    sleep 1
    if curl -s http://localhost:8090/api/status > /dev/null 2>&1; then
        echo "  [OK] Сервер готов!"
        echo ""
        echo "  Открываю http://localhost:8090 ..."
        echo "  (закройте это окно для остановки сервера)"
        echo ""
        open http://localhost:8090
        wait $SERVER_PID
        exit 0
    fi
done

echo "  [!] Сервер не запустился за 15 секунд."
kill $SERVER_PID 2>/dev/null
read -p "  Нажмите Enter для выхода..."
