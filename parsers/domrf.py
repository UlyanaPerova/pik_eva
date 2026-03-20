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
    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "domrf.yaml"
        super().__init__(config_path)

    async def parse_all(self) -> list[StorehouseItem]:
        from playwright.async_api import async_playwright

        items: list[StorehouseItem] = []
        api_cfg = self.config.get("api", {})
        base_url = self.config["base_url"]
        page_size = api_cfg.get("page_size", 500)

        target = self.config.get("target_types", {})
        always_types = set(target.get("always", []))
        by_area_cfg = target.get("by_area", {})
        by_area_types = set(by_area_cfg.get("types", []))
        max_area = by_area_cfg.get("max_area", 15)

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
                complex_name = link_info["complex_name"]
                developer = link_info.get("developer", "")
                city = link_info.get("city", "Казань")

                # Открываем страницу объекта — проходим anti-bot + WAF
                obj_url = f"{base_url}/сервисы/каталог-новостроек/объект/{object_id}"
                self.log.info("Загрузка страницы %s ...", obj_url)
                await page.goto(obj_url, timeout=60000, wait_until="domcontentloaded")
                await page.wait_for_timeout(8000)

                object_items = await self._parse_object(
                    page, base_url, api_cfg, object_id,
                    complex_name, developer, city, page_size,
                    always_types, by_area_types, max_area,
                )
                items.extend(object_items)
                await asyncio.sleep(1)

            await browser.close()

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

            # Корпус и номер из odsId: "61962/8/Н12" → building="8", number="Н12"
            building = ""
            item_number = ""
            if ods_id:
                parts = ods_id.split("/")
                if len(parts) >= 3:
                    building = parts[1]
                    item_number = parts[2]
                elif len(parts) == 2:
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
            )
        except (ValueError, KeyError, TypeError) as e:
            self.log.warning("Ошибка парсинга помещения: %s — %s", e, premise)
            return None
