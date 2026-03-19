"""
Парсер кладовок Ак Бар Дом (akbars-dom.ru).

Серверный рендеринг, пагинация через ?page=N (12 шт/стр).
Данные извлекаются из HTML-карточек. Номер кладовки — из data-floor-pop.
"""
from __future__ import annotations

import asyncio
import re
from pathlib import Path

import httpx

from parsers.base import BaseParser, StorehouseItem, logger

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"


class AkBarsDomParser(BaseParser):
    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "akbarsdom.yaml"
        super().__init__(config_path)

    async def parse_all(self) -> list[StorehouseItem]:
        items: list[StorehouseItem] = []
        async with httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"},
            follow_redirects=True,
        ) as client:
            for link_info in self.config.get("links", []):
                url = link_info["url"]
                city = link_info.get("city", self.config.get("city", "Казань"))
                page_items = await self._parse_all_pages(client, url, city)
                items.extend(page_items)

        self.log.info("Итого %s: %d кладовок", self.site_name, len(items))
        return items

    async def _parse_all_pages(
        self, client: httpx.AsyncClient, base_url: str, city: str
    ) -> list[StorehouseItem]:
        """Пройти все страницы пагинации."""
        all_items: list[StorehouseItem] = []

        # Первая страница — определяем общее количество страниц
        page1_html = await self._fetch_page(client, base_url, 1)
        total_pages = self._get_total_pages(page1_html)
        items = self._parse_page_html(page1_html, city)
        all_items.extend(items)
        self.log.info("  Страница 1/%d: %d кладовок", total_pages, len(items))

        # Остальные страницы — параллельно пачками по 5
        batch_size = 5
        for batch_start in range(2, total_pages + 1, batch_size):
            batch_end = min(batch_start + batch_size, total_pages + 1)
            tasks = [
                self._fetch_page(client, base_url, page)
                for page in range(batch_start, batch_end)
            ]
            pages_html = await asyncio.gather(*tasks, return_exceptions=True)
            for i, html in enumerate(pages_html):
                page_num = batch_start + i
                if isinstance(html, Exception):
                    self.log.error("  Ошибка на странице %d: %s", page_num, html)
                    continue
                items = self._parse_page_html(html, city)
                all_items.extend(items)
            self.log.info(
                "  Страницы %d-%d: загружено, итого %d",
                batch_start, batch_end - 1, len(all_items),
            )

        return all_items

    async def _fetch_page(
        self, client: httpx.AsyncClient, base_url: str, page: int
    ) -> str:
        """Загрузить одну страницу пагинации."""
        sep = "&" if "?" in base_url else "?"
        url = f"{base_url}{sep}page={page}"
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.text

    def _get_total_pages(self, html: str) -> int:
        """Определить общее число страниц из пагинации."""
        # Ищем максимальный номер страницы в ссылках пагинации
        pages = re.findall(r'\?page=(\d+)', html)
        if pages:
            return max(int(p) for p in pages)
        return 1

    def _parse_page_html(self, html: str, city: str) -> list[StorehouseItem]:
        """Парсить HTML-страницу и извлечь карточки кладовок."""
        items: list[StorehouseItem] = []

        # Извлекаем карточки: <a ... href="/storeroom/ID/" ...>...</a>
        card_pattern = re.compile(
            r'<a[^>]*href="/storeroom/(\d+)/"[^>]*class="[^"]*appartment-filter__room[^"]*"[^>]*>'
            r'(.*?)</a>',
            re.DOTALL,
        )
        # Также пробуем обратный порядок атрибутов
        card_pattern2 = re.compile(
            r'<a[^>]*class="[^"]*appartment-filter__room[^"]*"[^>]*href="/storeroom/(\d+)/"[^>]*>'
            r'(.*?)</a>',
            re.DOTALL,
        )

        cards = card_pattern.findall(html)
        if not cards:
            cards = card_pattern2.findall(html)
        if not cards:
            # Fallback: find all storeroom links and grab content between them
            cards = self._extract_cards_fallback(html)

        for item_id, card_html in cards:
            item = self._parse_card(item_id, card_html, city)
            if item:
                items.append(item)

        return items

    def _extract_cards_fallback(self, html: str) -> list[tuple[str, str]]:
        """Запасной метод: ищем все ссылки /storeroom/ID/ и берём контент между ними."""
        link_pattern = re.compile(
            r'<a[^>]*href="/storeroom/(\d+)/"[^>]*>(.*?)</a>',
            re.DOTALL,
        )
        return link_pattern.findall(html)

    def _parse_card(
        self, item_id: str, card_html: str, city: str
    ) -> StorehouseItem | None:
        """Разобрать одну карточку кладовки."""
        # Адрес / корпус
        title_m = re.search(
            r'appartment-filter__room_list-room-title["\']?>(.*?)<', card_html
        )
        address = title_m.group(1).strip() if title_m else ""

        # ЖК
        jk_m = re.search(
            r'appartment-filter__room_list-room-jk["\']?>(.*?)<', card_html
        )
        complex_name = jk_m.group(1).strip() if jk_m else ""

        # Площадь
        area_m = re.search(
            r'appartment-filter__room_list-square_title["\']?>(.*?)<', card_html
        )
        area_text = area_m.group(1).strip() if area_m else "0"
        area = self._parse_float(area_text)

        # Цена
        price_m = re.search(
            r'appartment-filter__room_list-price-main["\']?>(.*?)<', card_html
        )
        price_text = price_m.group(1).strip() if price_m else "0"
        price = self._parse_float(price_text)

        # Номер кладовки из data-floor-pop="Кладовка №2к"
        number_m = re.search(r'data-floor-pop="[^"]*№([^"]+)"', card_html)
        item_number = number_m.group(1).strip() if number_m else None

        # Корпус — извлекаем из адреса (например, "ул. Радужная УБ-2" → "УБ-2")
        building = self._extract_building(address)

        if not price or not area:
            return None

        price_per_meter = round(price / area, 2) if area > 0 else 0

        url = f"{self.config['base_url']}/storeroom/{item_id}/"

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

    # Маппинг адресов без стандартного кода корпуса → код корпуса
    ADDRESS_TO_BUILDING: dict[str, str] = {
        "Назиба Жиганова 2А": "М1/ПК-5",
        "Петра Полушкина 4": "ПК-2",
        "Петра Полушкина 1": "ПК-10",
    }

    def _extract_building(self, address: str) -> str:
        """Извлечь название корпуса из адреса.

        'ул. Радужная УБ-2' → 'УБ-2'
        'Проспект Победы М1/ПК-1 паркинг' → 'М1/ПК-1'
        'Назиба Жиганова 2А' → 'М1/ПК-5'  (маппинг)
        """
        # Сначала проверяем маппинг
        addr_clean = address.strip()
        if addr_clean in self.ADDRESS_TO_BUILDING:
            return self.ADDRESS_TO_BUILDING[addr_clean]

        # Ищем код корпуса: УБ-N, М1/ПК-N, ПК-N и т.п.
        m = re.search(r'((?:М\d*/)?(?:УБ|ПК)[\-\d]+)', address, re.IGNORECASE)
        if m:
            return m.group(1)

        # Fallback — логируем и возвращаем адрес целиком
        self.log.warning("Не удалось извлечь корпус из адреса: '%s'", address)
        return address

    @staticmethod
    def _parse_float(text: str) -> float:
        """Извлечь число из строки типа '454 118 ₽' или '3.26 м²'."""
        cleaned = re.sub(r'[^\d.,]', '', text)
        cleaned = cleaned.replace(',', '.')
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
