"""
Парсер кладовок СМУ-88 (smu88.group).

Nuxt3 SPA — данные рендерятся Vue на клиенте.
Используем Playwright: кликаем «Показать ещё» до конца,
затем извлекаем данные из DOM через page.evaluate().

На сайте: «проект» = ЖК, «место» = номер кладовки.
Корпус не отображается на карточках — оставляем пустым.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from parsers.base import BaseParser, StorehouseItem, logger

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"


class Smu88Parser(BaseParser):
    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "smu88.yaml"
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
        """Загрузить страницу через Playwright, кликнуть все 'Показать ещё', извлечь данные."""
        from playwright.async_api import async_playwright

        items: list[StorehouseItem] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            self.log.info("Загрузка %s ...", url)
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)

            # Кликаем «Показать ещё» до исчезновения кнопки
            click_count = 0
            while True:
                has_btn = await page.evaluate("""() => {
                    const btn = document.querySelector('.pantry-page-list__pagination');
                    return btn && btn.offsetParent !== null;
                }""")
                if not has_btn:
                    break

                await page.evaluate(
                    "document.querySelector('.pantry-page-list__pagination').click()"
                )
                await page.wait_for_timeout(1500)
                click_count += 1

                if click_count > 30:  # защита от бесконечного цикла
                    self.log.warning("Превышен лимит кликов (30)")
                    break

            self.log.info("  Кликов «Показать ещё»: %d", click_count)

            # Извлекаем данные из DOM
            raw_cards = await page.evaluate("""() => {
                const cards = [];
                document.querySelectorAll('.pantry-page-list__items > div').forEach(item => {
                    const ps = item.querySelectorAll('p');
                    if (ps.length >= 6) {
                        cards.push({
                            project: ps[0]?.textContent?.trim() || '',
                            price: ps[1]?.textContent?.replace(/[^\\d]/g, '') || '0',
                            area: ps[2]?.textContent?.replace(/[^\\d.]/g, '') || '0',
                            floor: ps[3]?.textContent?.trim() || '',
                            number: ps[4]?.textContent?.replace(/[^\\d]/g, '') || '',
                            date: ps[5]?.textContent?.trim() || '',
                        });
                    }
                });
                return cards;
            }""")

            await browser.close()

        self.log.info("  Карточек из DOM: %d", len(raw_cards))

        # Конвертируем в StorehouseItem
        for card in raw_cards:
            try:
                price = float(card["price"]) if card["price"] else 0
                area = float(card["area"]) if card["area"] else 0
                number = card["number"]
                project = card["project"]

                if price <= 0 or area <= 0:
                    continue

                price_per_meter = round(price / area, 2)

                # ID: уникальный по проекту + номеру
                item_id = f"smu88_{project}_{number}"

                items.append(StorehouseItem(
                    site=self.site_key,
                    city=city,
                    complex_name=project,
                    building="",  # на сайте нет корпуса
                    item_id=item_id,
                    area=area,
                    price=price,
                    price_per_meter=price_per_meter,
                    url=f"{self.config['base_url']}/kladovye",
                    item_number=number,
                ))
            except (ValueError, KeyError) as e:
                self.log.warning("Ошибка парсинга карточки: %s", e)

        # Логирование по ЖК
        from collections import Counter
        jk_counts = Counter(it.complex_name for it in items)
        for jk, cnt in sorted(jk_counts.items()):
            self.log.info("    %s: %d кладовок", jk, cnt)

        return items
