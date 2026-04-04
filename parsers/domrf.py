"""
Парсер кладовок с наш.дом.рф (ЕИСЖС).

Стратегия: browser (Playwright) + API.
Сайт защищён ServicePipe — нужен браузер для прохождения JS-challenge.
После прохождения challenge вызываем API из контекста браузера через page.evaluate(fetch).

Endpoint: /portal-kn/api/kn/objects/{objectId}/flats?flatGroupType=premises

Фильтрация по типам помещений:
  - «Кладовая» — берём всегда
  - «Келлер» — берём всегда
  - «Нежилое помещение» (без уточнения) — только если площадь ≤ 15 м²
  - «Нежилое помещение для коммерческого использования» и подобные — пропускаем

Цены на дом.рф часто отсутствуют (null) — такие помещения всё равно берём,
цена и цена/м² записываются как 0.
"""
from __future__ import annotations

import asyncio
import json
from collections import Counter
from pathlib import Path

from parsers.base import BaseParser, StorehouseItem

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"


class DomRfParser(BaseParser):
    def __init__(self, config_path: str | Path | None = None, cdp_port: int | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "domrf.yaml"
        super().__init__(config_path)
        self.cdp_port = cdp_port

    async def parse_all(self) -> list[StorehouseItem]:
        if self.cdp_port:
            return await self._parse_all_cdp()
        return await self._parse_all_headless()

    async def _parse_all_cdp(self) -> list[StorehouseItem]:
        """Парсинг через CDP (настоящий Chrome) с обработкой капчи и resume."""
        from parsers.cdp_browser import CdpBrowser
        from parsers.base import init_db, save_items

        items: list[StorehouseItem] = []
        api_cfg = self.config.get("api", {})
        base_url = self.config["base_url"]
        page_size = api_cfg.get("page_size", 500)

        target = self.config.get("target_types", {})
        always_types = set(target.get("always", []))
        by_area_cfg = target.get("by_area", {})
        by_area_types = set(by_area_cfg.get("types", []))
        max_area = by_area_cfg.get("max_area", 15)

        links = self.config.get("links", [])
        cdp = CdpBrowser(
            port=self.cdp_port,
            checkpoint_key=f"{self.site_key}_storehouses",
            config_links=links,
        )
        await cdp.connect()
        page = cdp.page

        # Инкрементальное сохранение — открываем БД сразу
        conn = init_db()

        try:
            for link_info in links:
                object_id = link_info["object_id"]
                building_from_config = link_info.get("building", "")
                complex_name = link_info["complex_name"]
                developer = link_info.get("developer", "")
                city = link_info.get("city", "Казань")

                # Resume: пропускаем уже обработанные
                if cdp.is_completed(object_id):
                    self.log.info("Пропуск %d — %s (уже обработан)", object_id, complex_name)
                    continue

                obj_url = f"{base_url}/сервисы/каталог-новостроек/объект/{object_id}"
                self.log.info("Загрузка страницы %s ...", obj_url)

                if not await cdp.goto(obj_url, object_id):
                    continue  # капча не решена — пропускаем

                object_items = await self._parse_object(
                    page, base_url, api_cfg, object_id,
                    complex_name, developer, city, page_size,
                    always_types, by_area_types, max_area,
                    building_from_config=building_from_config,
                )
                items.extend(object_items)

                # Инкрементальное сохранение после каждого объекта
                if object_items:
                    save_items(conn, object_items)
                    self.log.info("  💾 Сохранено %d кладовок в БД", len(object_items))

                cdp.mark_completed(object_id)
                await asyncio.sleep(1)

            cdp.clear_checkpoint()

        finally:
            conn.close()
            await cdp.close()

        self.log.info("Итого %s: %d кладовок", self.site_name, len(items))
        return items

    async def _parse_all_headless(self) -> list[StorehouseItem]:
        """Парсинг через headless Playwright с инкрементальным сохранением в БД."""
        from playwright.async_api import async_playwright
        from parsers.base import init_db, save_items

        items: list[StorehouseItem] = []
        api_cfg = self.config.get("api", {})
        base_url = self.config["base_url"]
        page_size = api_cfg.get("page_size", 500)

        target = self.config.get("target_types", {})
        always_types = set(target.get("always", []))
        by_area_cfg = target.get("by_area", {})
        by_area_types = set(by_area_cfg.get("types", []))
        max_area = by_area_cfg.get("max_area", 15)

        # Инкрементальное сохранение — открываем БД сразу
        conn = init_db()
        errors = 0
        max_consecutive_errors = 3

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="ru-RU",
            )
            page = await context.new_page()
            await page.add_init_script(
                'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
            )

            for link_info in self.config.get("links", []):
                object_id = link_info["object_id"]
                building_from_config = link_info.get("building", "")
                complex_name = link_info["complex_name"]
                developer = link_info.get("developer", "")
                city = link_info.get("city", "Казань")

                try:
                    # Открываем страницу объекта — проходим anti-bot + WAF
                    obj_url = f"{base_url}/сервисы/каталог-новостроек/объект/{object_id}"
                    self.log.info("Загрузка страницы %s ...", obj_url)
                    await page.goto(obj_url, timeout=60000, wait_until="domcontentloaded")
                    await page.wait_for_timeout(8000)

                    object_items = await self._parse_object(
                        page, base_url, api_cfg, object_id,
                        complex_name, developer, city, page_size,
                        always_types, by_area_types, max_area,
                        building_from_config=building_from_config,
                    )
                    items.extend(object_items)

                    # Инкрементальное сохранение после каждого объекта
                    if object_items:
                        save_items(conn, object_items)
                        self.log.info("  💾 Сохранено %d кладовок в БД", len(object_items))

                    errors = 0  # сброс счётчика ошибок после успеха
                    await asyncio.sleep(2)

                except Exception as e:
                    errors += 1
                    self.log.error(
                        "  ❌ Ошибка объекта %d (%s): %s", object_id, complex_name, e
                    )
                    if errors >= max_consecutive_errors:
                        self.log.error(
                            "  ⛔ %d подряд ошибок — останавливаем парсинг", errors
                        )
                        break
                    # Переоткрываем страницу при ошибке (может помочь с anti-bot)
                    try:
                        await page.goto("about:blank", timeout=10000)
                        await asyncio.sleep(5)
                    except Exception:
                        pass

            await browser.close()

        conn.close()
        self.log.info("Итого %s: %d кладовок", self.site_name, len(items))
        return items

    async def _parse_object(
        self,
        page,
        base_url: str,
        api_cfg: dict,
        object_id: int,
        complex_name: str,
        developer: str,
        city: str,
        page_size: int,
        always_types: set[str],
        by_area_types: set[str],
        max_area: float,
        building_from_config: str = "",
    ) -> list[StorehouseItem]:
        """Загрузить все нежилые помещения одного объекта через fetch из браузера."""
        flats_ep = api_cfg.get(
            "flats_endpoint",
            "/portal-kn/api/kn/objects/{object_id}/flats",
        ).format(object_id=object_id)

        all_premises: list[dict] = []
        offset = 0
        total = None

        while True:
            api_url = (
                f"{base_url}{flats_ep}"
                f"?flatGroupType=premises&limit={page_size}&offset={offset}"
            )
            self.log.debug("fetch %s", api_url)

            # Вызываем API из контекста браузера (с cookie от ServicePipe)
            raw = await page.evaluate(
                """async (url) => {
                    const resp = await fetch(url);
                    if (!resp.ok) return {error: resp.status, text: await resp.text()};
                    return await resp.json();
                }""",
                api_url,
            )

            if isinstance(raw, dict) and "error" in raw:
                self.log.error(
                    "API вернул %s для объекта %d: %s",
                    raw["error"], object_id, raw.get("text", "")[:200],
                )
                break

            batch = raw.get("data", [])
            if total is None:
                total = raw.get("total", len(batch))
                self.log.info(
                    "  %s (объект %d): всего %d нежилых помещений",
                    complex_name, object_id, total,
                )

            all_premises.extend(batch)
            offset += len(batch)

            if not batch or offset >= total:
                break
            await asyncio.sleep(0.5)

        # Фильтрация по типу и площади
        items: list[StorehouseItem] = []
        skipped: Counter = Counter()
        for premise in all_premises:
            ptype = (premise.get("type") or "").strip()
            area = premise.get("totalArea") or 0

            if ptype in always_types:
                pass  # берём
            elif ptype in by_area_types and area <= max_area:
                pass  # берём
            else:
                skipped[ptype] += 1
                continue

            item = self._parse_premise(
                premise, object_id, complex_name, developer, city, base_url,
                building_from_config=building_from_config,
            )
            if item:
                items.append(item)

        for ptype, cnt in sorted(skipped.items()):
            self.log.debug("    Пропущено: %s — %d шт.", ptype, cnt)

        # Логирование по корпусам
        building_counts = Counter(it.building for it in items)
        for bld, cnt in sorted(building_counts.items()):
            self.log.info("    Корпус %s: %d кладовок", bld, cnt)

        self.log.info(
            "  %s: отобрано %d из %d помещений",
            complex_name, len(items), len(all_premises),
        )
        return items

    def _parse_premise(
        self,
        premise: dict,
        object_id: int,
        complex_name: str,
        developer: str,
        city: str,
        base_url: str,
        building_from_config: str = "",
    ) -> StorehouseItem | None:
        """Преобразовать одно помещение из API в StorehouseItem."""
        try:
            elem_id = premise.get("elemId", "")
            ods_id = premise.get("odsId", "")
            area = float(premise.get("totalArea") or 0)
            entrance = premise.get("entranceNumber")

            # Цены на дом.рф отсутствуют — оставляем пустыми (0)
            price = 0
            price_per_meter = 0

            # Корпус: берём из конфига если задан, иначе из odsId
            # (в odsId второй сегмент — внутренний код категории ДОМ.РФ, не номер корпуса)
            item_number = ""
            if ods_id:
                parts = ods_id.split("/")
                if len(parts) >= 3:
                    item_number = parts[2]
                elif len(parts) == 2:
                    item_number = parts[1]

            if building_from_config:
                building = building_from_config
            else:
                # Fallback: используем второй сегмент odsId (старое поведение)
                building = ""
                if ods_id:
                    parts = ods_id.split("/")
                    if len(parts) >= 2:
                        building = parts[1]

            # Подъезд → в примечание к корпусу через ||
            if entrance:
                building = f"{building}||подъезд {entrance}"

            # item_id — уникальный, берём elemId (хеш)
            item_id = elem_id or f"domrf_{object_id}_{ods_id}"

            # URL на страницу кладовки
            url = f"{base_url}/сервисы/каталог-квартир/квартира/{elem_id}"

            return StorehouseItem(
                site=self.site_key,
                city=city,
                complex_name=complex_name,
                building=building,
                item_id=item_id,
                area=area,
                price=price,
                price_per_meter=price_per_meter,
                url=url,
                item_number=item_number,
                developer=developer,
                object_id=object_id,
            )
        except (ValueError, KeyError, TypeError) as e:
            self.log.warning("Ошибка парсинга помещения: %s — %s", e, premise)
            return None
