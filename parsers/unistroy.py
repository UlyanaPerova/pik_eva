"""
Парсер кладовок УниСтрой (unistroyrf.ru).

Next.js SSR — данные загружаются через внутренний API uos.unistroyrf.ru.
API требует авторизацию для полного доступа (без авторизации — макс. 8 кладовок на объект).
Используем Playwright: загружаем страницу и перехватываем POST /storage/list.

ОГРАНИЧЕНИЕ: без авторизации доступно ~74 кладовок из 575.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from parsers.base import BaseParser, StorehouseItem, logger

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"


class UnistroyParser(BaseParser):
    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "unistroy.yaml"
        super().__init__(config_path)

    async def parse_all(self) -> list[StorehouseItem]:
        items: list[StorehouseItem] = []
        for link_info in self.config.get("links", []):
            url = link_info["url"]
            city = link_info.get("city", "Казань")
            page_items = await self._parse_with_playwright(url, city)
            items.extend(page_items)

        self.log.info("Итого %s: %d кладовок", self.site_name, len(items))
        return items

    async def _parse_with_playwright(
        self, url: str, city: str
    ) -> list[StorehouseItem]:
        """Загрузить страницу через Playwright, перехватить API-ответ."""
        from playwright.async_api import async_playwright

        items: list[StorehouseItem] = []
        raw_apartments: dict[str, dict] = {}

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            async def on_resp(response):
                if "storage/list" in response.url and response.request.method == "POST":
                    try:
                        result = await response.json()
                        data = result.get("data", {})
                        for cc, cdata in data.items():
                            cname = cdata.get("complex_name", cc)
                            for oc, odata in cdata.get("objects", {}).items():
                                oname = odata.get("object_name", oc)
                                for apt in odata.get("apartments", []):
                                    uid = apt.get("apartment_ui", "")
                                    if uid:
                                        apt["_cname"] = cname
                                        apt["_oname"] = oname
                                        raw_apartments[uid] = apt
                    except Exception:
                        pass

            page.on("response", on_resp)

            self.log.info("Загрузка %s ...", url)
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(10000)
            self.log.info("  После загрузки: %d кладовок", len(raw_apartments))

            # Кликаем все кнопки «Показать ещё N из M клад.» через JS
            for click_num in range(100):
                clicked = await page.evaluate("""() => {
                    const buttons = document.querySelectorAll('button');
                    for (const btn of buttons) {
                        const text = btn.textContent.trim();
                        if (text.includes('Показать') && text.includes('из') && text.includes('клад')) {
                            btn.scrollIntoView();
                            btn.click();
                            return text;
                        }
                    }
                    return null;
                }""")

                if not clicked:
                    break

                await page.wait_for_timeout(2000)

                if click_num % 10 == 9:
                    self.log.info("    Клик %d: %d кладовок", click_num + 1, len(raw_apartments))

            self.log.info("  Кликов «Показать ещё»: %d, всего: %d", click_num, len(raw_apartments))

            await browser.close()

        self.log.info("  Перехвачено из API: %d кладовок", len(raw_apartments))

        # Конвертируем в StorehouseItem
        for uid, apt in raw_apartments.items():
            try:
                price = float(apt.get("apartment_cost", 0))
                area = float(apt.get("square", 0) or apt.get("apart_square", 0))

                if price <= 0 or area <= 0:
                    continue

                price_per_meter = round(price / area, 2)
                complex_name = apt.get("_cname", "")
                # Убираем "Жилой комплекс" из названия
                for prefix in ['Жилой комплекс ', 'Жилой комплекс "', 'ЖК ']:
                    if complex_name.startswith(prefix):
                        complex_name = complex_name[len(prefix):]
                        break
                complex_name = complex_name.strip().strip('"')

                house = str(apt.get("house", ""))
                object_name = apt.get("_oname", "")
                # Корпус: "Дом №2" → "2", "Корпус №1.1" → "1.1", "Парковка" → "Парковка"
                building = house if house else object_name

                apart_number = str(apt.get("apart_number", ""))

                items.append(StorehouseItem(
                    site=self.site_key,
                    city=city,
                    complex_name=complex_name,
                    building=building,
                    item_id=uid,
                    area=area,
                    price=price,
                    price_per_meter=price_per_meter,
                    url=f"https://unistroyrf.ru/storage/",
                    item_number=apart_number,
                ))
            except (ValueError, KeyError) as e:
                self.log.warning("Ошибка парсинга: %s", e)

        # Логирование по ЖК
        from collections import Counter
        jk_counts = Counter(it.complex_name for it in items)
        for jk, cnt in sorted(jk_counts.items()):
            self.log.info("    %s: %d кладовок", jk, cnt)

        return items
