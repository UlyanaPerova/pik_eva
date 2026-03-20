"""
Парсер квартир СМУ-88 (smu88.group).

Nuxt3 SPA — данные рендерятся Vue на клиенте.
Используем Playwright: кликаем «Показать ещё» до конца,
затем извлекаем данные из DOM через page.evaluate().

Карточки: .premise-card
Текст: "ЖК | Дом/Корпус | Этаж Квартал Комнаты Площадь Цена"
Ссылки: /kvartiry/ID
Пагинация: .premises-page__pagination
"""
from __future__ import annotations

import asyncio
import re
from collections import Counter
from pathlib import Path

from parsers.apartments_base import BaseApartmentParser, ApartmentItem, logger

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"


class Smu88ApartmentParser(BaseApartmentParser):
    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "smu88.yaml"
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
        """Загрузить страницу через Playwright, кликнуть все 'Показать ещё', извлечь данные."""
        from playwright.async_api import async_playwright

        items: list[ApartmentItem] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            self.log.info("Загрузка %s ...", url)
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)

            # Кликаем «Показать ещё» до исчезновения кнопки
            click_count = 0
            while True:
                has_btn = await page.evaluate(r"""() => {
                    const el = document.querySelector('.premises-page__pagination');
                    return el && el.offsetParent !== null;
                }""")
                if not has_btn:
                    break

                await page.evaluate(r"""() => {
                    const el = document.querySelector('.premises-page__pagination');
                    if (el) el.click();
                }""")
                await page.wait_for_timeout(1500)
                click_count += 1

                if click_count % 10 == 0:
                    self.log.info("    Клик %d", click_count)

                if click_count > 200:
                    self.log.warning("Превышен лимит кликов (200)")
                    break

            self.log.info("  Кликов «Показать ещё»: %d", click_count)

            # Извлекаем данные из DOM
            raw_cards = await page.evaluate(r"""() => {
                const cards = [];
                document.querySelectorAll('.premise-card').forEach(card => {
                    const link = card.querySelector('a[href]');
                    const href = link ? link.getAttribute('href') : '';
                    const text = card.textContent.replace(/\s+/g, ' ').trim();
                    cards.push({ href, text });
                });
                return cards;
            }""")

            await browser.close()

        self.log.info("  Карточек из DOM: %d", len(raw_cards))

        # Парсим текст карточек
        # Формат: "ЖК | Корпус | Этаж Квартал Комнаты Площадь Цена Доп.инфо"
        for card in raw_cards:
            try:
                text = card["text"]
                href = card.get("href", "")

                # Парсим ЖК и корпус: "ЖК | Корпус | остальное"
                parts = text.split("|")
                if len(parts) < 2:
                    continue

                complex_name = parts[0].strip()
                building_raw = parts[1].strip()
                rest = "|".join(parts[2:]).strip() if len(parts) > 2 else ""

                # Этаж: ищем "N этаж"
                floor_m = re.search(r'(\d+)\s*этаж', rest, re.IGNORECASE)
                floor = int(floor_m.group(1)) if floor_m else 0

                # Комнаты: "1-комнатная", "2-комнатная", "студия"
                if "студи" in text.lower():
                    rooms = 0
                else:
                    rooms_m = re.search(r'(\d)\s*[-\s]?комн', text, re.IGNORECASE)
                    rooms = int(rooms_m.group(1)) if rooms_m else 0

                # Площадь: "65.92 м²"
                area_m = re.search(r'([\d.,]+)\s*м²', text)
                if not area_m:
                    continue
                area = float(area_m.group(1).replace(",", "."))

                # Цена: последовательность цифр и пробелов перед "₽"
                price_m = re.search(r'([\d\s]+)\s*₽', text)
                if not price_m:
                    # Fallback: ищем число > 1 000 000
                    price_m = re.search(r'([\d\s]{7,})', text.replace("\xa0", " "))
                if not price_m:
                    continue
                price = float(price_m.group(1).replace(" ", "").replace("\xa0", ""))

                if price <= 0 or area <= 0:
                    continue

                price_per_meter = round(price / area, 2)

                # ID из URL: /kvartiry/16401
                item_id_m = re.search(r'/kvartiry/(\d+)', href)
                item_id = f"smu88_apt_{item_id_m.group(1)}" if item_id_m else f"smu88_apt_{complex_name}_{rooms}_{area}_{price}"

                # URL
                if href and href.startswith("/"):
                    item_url = f"{self.config['base_url']}{href}"
                else:
                    item_url = f"{self.config['base_url']}/kvartiry"

                items.append(ApartmentItem(
                    site=self.site_key,
                    city=city,
                    complex_name=complex_name,
                    building=building_raw,
                    item_id=item_id,
                    rooms=rooms,
                    floor=floor,
                    area=area,
                    price=price,
                    price_per_meter=price_per_meter,
                    url=item_url,
                ))
            except (ValueError, KeyError) as e:
                self.log.warning("Ошибка парсинга карточки: %s", e)

        # Лог
        jk_counts = Counter((it.complex_name, it.rooms_label) for it in items)
        for (jk, rl), cnt in sorted(jk_counts.items()):
            self.log.info("    %s / %s: %d квартир", jk, rl, cnt)

        return items
