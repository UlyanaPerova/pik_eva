"""
Парсер квартир УниСтрой (unistroyrf.ru).

Стратегия: Playwright + перехват POST-ответов API.
Аналогично парсеру кладовок, но перехватываем /search/list вместо /storage/list.
Кликаем «Показать ещё» для каждого блока ЖК.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from parsers.apartments_base import BaseApartmentParser, ApartmentItem, logger

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"


class UnistroyApartmentParser(BaseApartmentParser):
    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "unistroy.yaml"
        super().__init__(config_path)

    async def parse_all(self) -> list[ApartmentItem]:
        items: list[ApartmentItem] = []
        for link_info in self.config.get("apartment_links", []):
            url = link_info["url"]
            city = link_info.get("city", "Казань")
            page_items = await self._parse_with_playwright(url, city)
            items.extend(page_items)

        self.log.info("Итого %s квартиры: %d", self.site_name, len(items))
        return items

    async def _parse_with_playwright(
        self, url: str, city: str
    ) -> list[ApartmentItem]:
        """Загрузить страницу через Playwright, перехватить API-ответ."""
        from playwright.async_api import async_playwright

        items: list[ApartmentItem] = []
        raw_apartments: dict[str, dict] = {}

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            async def on_resp(response):
                # Перехватываем POST-запросы к API квартир
                url_lower = response.url.lower()
                is_apt_api = (
                    ("search/list" in url_lower or "apartment/list" in url_lower
                     or "flat/list" in url_lower)
                    and response.request.method == "POST"
                )
                if is_apt_api:
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
            self.log.info("  После загрузки: %d квартир", len(raw_apartments))

            # Кликаем все кнопки «Показать ещё»
            for click_num in range(200):
                clicked = await page.evaluate("""() => {
                    const buttons = document.querySelectorAll('button');
                    for (const btn of buttons) {
                        const text = btn.textContent.trim();
                        if (text.includes('Показать') && text.includes('из') &&
                            (text.includes('кв') || text.includes('кварт') || text.includes('объект') || text.includes('предл'))) {
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
                    self.log.info("    Клик %d: %d квартир",
                                 click_num + 1, len(raw_apartments))

            self.log.info("  Кликов «Показать ещё»: %d, всего: %d",
                          click_num + 1 if 'click_num' in dir() else 0,
                          len(raw_apartments))

            await browser.close()

        self.log.info("  Перехвачено из API: %d квартир", len(raw_apartments))

        # Конвертируем в ApartmentItem
        for uid, apt in raw_apartments.items():
            try:
                price = float(apt.get("apartment_cost", 0))
                area = float(apt.get("square", 0) or apt.get("apart_square", 0))

                if price <= 0 or area <= 0:
                    continue

                price_per_meter = round(price / area, 2)

                complex_name = apt.get("_cname", "")
                for prefix in ['Жилой комплекс ', 'Жилой комплекс "', 'ЖК ']:
                    if complex_name.startswith(prefix):
                        complex_name = complex_name[len(prefix):]
                        break
                complex_name = complex_name.strip().strip('"')

                house = str(apt.get("house", ""))
                object_name = apt.get("_oname", "")
                building = house if house else object_name

                apart_number = str(apt.get("apart_number", ""))

                # Комнаты
                rooms_raw = apt.get("rooms_count", apt.get("rooms", 0))
                subtype = apt.get("apartment_subtype_name", "") or apt.get("subtype_name", "") or ""
                subtype = subtype.lower()
                if "студи" in subtype:
                    rooms = 0
                else:
                    try:
                        rooms = int(rooms_raw)
                    except (ValueError, TypeError):
                        rooms = 0

                # Этаж
                try:
                    floor = int(apt.get("floor", 0))
                except (ValueError, TypeError):
                    floor = 0

                items.append(ApartmentItem(
                    site=self.site_key,
                    city=city,
                    complex_name=complex_name,
                    building=building,
                    item_id=uid,
                    rooms=rooms,
                    floor=floor,
                    area=area,
                    price=price,
                    price_per_meter=price_per_meter,
                    url=f"https://unistroyrf.ru/search/placement/{uid}/",
                    apartment_number=apart_number,
                ))
            except (ValueError, KeyError) as e:
                self.log.warning("Ошибка парсинга: %s", e)

        # Лог
        from collections import Counter
        jk_counts = Counter((it.complex_name, it.rooms_label) for it in items)
        for (jk, rl), cnt in sorted(jk_counts.items()):
            self.log.info("    %s / %s: %d квартир", jk, rl, cnt)

        return items
