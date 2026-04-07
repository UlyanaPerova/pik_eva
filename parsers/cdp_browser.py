"""
CDP-модуль для подключения к реальному Chrome.

Возможности:
  - Подключение к Chrome через remote-debugging-port (CDP)
  - Автоматический детект капчи / ServicePipe challenge
  - Ожидание ручного решения капчи пользователем (polling + таймаут)
  - Checkpoint/resume: запоминает обработанные объекты, при перезапуске
    пропускает уже готовые

Использование:
    cdp = CdpBrowser(port=9222, checkpoint_key="domrf")
    await cdp.connect()

    for obj_id in object_ids:
        if cdp.is_completed(obj_id):
            continue
        if not await cdp.goto(url, obj_id):
            continue  # капча не решена
        # ... парсинг через cdp.page ...
        cdp.mark_completed(obj_id)

    cdp.clear_checkpoint()
    await cdp.close()
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger("cdp_browser")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Признаки ServicePipe challenge / антибот-страницы
_CHALLENGE_MARKERS = [
    "exhkqyad",          # ServicePipe redirect path
    "servicepipe",       # ServicePipe scripts
    "cf-challenge",      # Cloudflare challenge (на всякий)
    "captcha",           # Generic captcha
    "ray-id",            # Cloudflare ray
]

# Таймауты (секунды)
_CAPTCHA_TIMEOUT = 120     # макс. ожидание решения капчи
_CAPTCHA_POLL_INTERVAL = 3  # интервал проверки
_PAGE_WAIT = 10            # ожидание после goto перед проверкой


class CdpBrowser:
    """Подключение к Chrome через CDP с обработкой капчи и resume."""

    def __init__(
        self,
        port: int,
        checkpoint_key: str | None = None,
        config_links: list[dict] | None = None,
        captcha_timeout: int = _CAPTCHA_TIMEOUT,
    ):
        """
        Args:
            port: порт remote-debugging Chrome
            checkpoint_key: ключ для файла checkpoint (напр. "domrf").
                Если None — checkpoint отключён.
            config_links: список links из конфига (для хеширования).
                Если конфиг изменился — checkpoint сбрасывается.
            captcha_timeout: сколько секунд ждать решения капчи.
        """
        self.port = port
        self.captcha_timeout = captcha_timeout

        # Playwright objects — заполняются в connect()
        self._playwright = None
        self._browser = None
        self.page = None

        # Checkpoint
        self._checkpoint_key = checkpoint_key
        self._checkpoint_path: Path | None = None
        self._completed_ids: set[str] = set()
        self._config_hash: str = ""

        if checkpoint_key:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            self._checkpoint_path = DATA_DIR / f".cdp_checkpoint_{checkpoint_key}.json"
            self._config_hash = self._hash_config(config_links or [])
            self._load_checkpoint()

    # ── Подключение ──────────────────────────────────────

    async def connect(self) -> None:
        """Подключиться к Chrome через CDP, получить page."""
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        log.info("Подключение к Chrome через CDP (порт %d)...", self.port)

        self._browser = await self._playwright.chromium.connect_over_cdp(
            f"http://127.0.0.1:{self.port}",
        )

        # Используем существующий контекст браузера (со всеми куками)
        if self._browser.contexts:
            context = self._browser.contexts[0]
        else:
            context = await self._browser.new_context()

        # Используем существующую вкладку если есть (антибот блокирует new_page)
        # Если нет открытых вкладок — создаём новую
        if context.pages:
            self.page = context.pages[0]
            log.info("CDP подключён, используем существующую вкладку (%s)", self.page.url[:60])
        else:
            self.page = await context.new_page()
            log.info("CDP подключён, создана новая вкладка")

    # ── Навигация с детектом капчи ───────────────────────

    async def goto(self, url: str, object_id: int | str = "") -> bool:
        """Перейти на url, проверить капчу, дождаться решения.

        Returns:
            True — страница загружена успешно.
            False — капча не решена за таймаут (объект пропущен).
        """
        import asyncio

        try:
            await self.page.goto(url, timeout=60000, wait_until="domcontentloaded")
        except Exception as e:
            log.warning("Ошибка загрузки %s: %s", url, e)
            return False

        await self.page.wait_for_timeout(_PAGE_WAIT * 1000)

        # Проверяем: капча/challenge?
        if not await self._is_challenge():
            return True

        # Капча обнаружена — ждём ручного решения
        log.warning(
            "⚠ Капча/challenge на странице (объект %s)! "
            "Решите её в браузере. Ожидание до %d сек...",
            object_id, self.captcha_timeout,
        )

        elapsed = 0
        while elapsed < self.captcha_timeout:
            await asyncio.sleep(_CAPTCHA_POLL_INTERVAL)
            elapsed += _CAPTCHA_POLL_INTERVAL

            if not await self._is_challenge():
                log.info("✓ Капча решена (объект %s), продолжаю", object_id)
                # Даём странице дозагрузиться
                await self.page.wait_for_timeout(3000)
                return True

            remaining = self.captcha_timeout - elapsed
            if remaining > 0 and remaining % 15 == 0:
                log.warning("  ... осталось %d сек", remaining)

        log.error(
            "✗ Капча не решена за %d сек (объект %s), пропускаю",
            self.captcha_timeout, object_id,
        )
        return False

    async def fetch_json(self, url: str) -> dict | list | None:
        """Выполнить fetch() в контексте браузера, вернуть JSON или None при ошибке."""
        raw = await self.page.evaluate(
            """async (url) => {
                const resp = await fetch(url);
                if (!resp.ok) return {__cdp_error: resp.status, text: await resp.text()};
                const text = await resp.text();
                try { return JSON.parse(text); }
                catch(e) { return {__cdp_error: 'not_json', text: text.substring(0, 300)}; }
            }""",
            url,
        )

        if isinstance(raw, dict) and "__cdp_error" in raw:
            log.error("fetch %s → %s: %s", url, raw["__cdp_error"], str(raw.get("text", ""))[:200])
            return None

        return raw

    # ── Checkpoint / Resume ──────────────────────────────

    def is_completed(self, object_id: int | str) -> bool:
        """Проверить, обработан ли объект в предыдущем запуске."""
        return str(object_id) in self._completed_ids

    def mark_completed(self, object_id: int | str) -> None:
        """Пометить объект как обработанный и сохранить checkpoint."""
        self._completed_ids.add(str(object_id))
        self._save_checkpoint()

    def clear_checkpoint(self) -> None:
        """Удалить checkpoint (вызывается при полном успешном завершении)."""
        if self._checkpoint_path and self._checkpoint_path.exists():
            self._checkpoint_path.unlink()
            log.info("Checkpoint удалён (%s)", self._checkpoint_path.name)
        self._completed_ids.clear()

    @property
    def completed_count(self) -> int:
        return len(self._completed_ids)

    # ── Завершение ───────────────────────────────────────

    async def close(self) -> None:
        """Отключиться от браузера (не закрываем вкладку — она пользовательская)."""
        # Не закрываем page — это может быть существующая вкладка пользователя
        self.page = None
        if self._playwright:
            await self._playwright.stop()

    # ── Внутренние методы ────────────────────────────────

    async def _is_challenge(self) -> bool:
        """Проверить, показывается ли challenge/captcha страница."""
        try:
            url = self.page.url.lower()
            for marker in _CHALLENGE_MARKERS:
                if marker in url:
                    return True

            html = await self.page.evaluate(
                "() => document.documentElement.outerHTML.substring(0, 3000).toLowerCase()"
            )

            for marker in _CHALLENGE_MARKERS:
                if marker in html:
                    return True

            # Дополнительная проверка: страница почти пустая (нет контента)
            body_len = await self.page.evaluate("() => (document.body?.innerText || '').length")
            if body_len < 50:
                return True

        except Exception as e:
            log.debug("Ошибка проверки challenge: %s", e)

        return False

    def _load_checkpoint(self) -> None:
        """Загрузить checkpoint с диска."""
        if not self._checkpoint_path or not self._checkpoint_path.exists():
            return

        try:
            data = json.loads(self._checkpoint_path.read_text(encoding="utf-8"))

            # Если конфиг изменился — сбрасываем
            if data.get("config_hash") != self._config_hash:
                log.info("Конфиг изменился — checkpoint сброшен")
                self._checkpoint_path.unlink()
                return

            self._completed_ids = set(str(x) for x in data.get("completed_ids", []))
            if self._completed_ids:
                log.info(
                    "Загружен checkpoint: %d объектов уже обработано (от %s)",
                    len(self._completed_ids),
                    data.get("started_at", "?"),
                )
        except (json.JSONDecodeError, KeyError) as e:
            log.warning("Невалидный checkpoint, сброс: %s", e)
            self._completed_ids.clear()

    def _save_checkpoint(self) -> None:
        """Сохранить checkpoint на диск."""
        if not self._checkpoint_path:
            return

        data = {
            "completed_ids": sorted(self._completed_ids),
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "config_hash": self._config_hash,
        }
        self._checkpoint_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _hash_config(links: list[dict]) -> str:
        """Хеш links-секции конфига для инвалидации checkpoint."""
        raw = json.dumps(links, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(raw.encode()).hexdigest()[:12]
