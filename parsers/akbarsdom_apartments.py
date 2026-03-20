"""
Парсер квартир Ак Бар Дом (akbars-dom.ru).

Серверный рендеринг, пагинация через ?page=N (11 шт/стр).
Данные извлекаются из HTML-карточек.

Структура карточки:
  <a href="/apartments/{ID}/" class="appartment-filter__room parent-el-share ">
    <div class="appartment-filter__room_header">
      <div class="appartment-filter__room_header-left">
        <div>{ЖК}</div>              <!-- complex_name -->
        <div>
          <span>{Адрес} •</span>      <!-- address / building -->
          <span>{Срок сдачи}</span>
        </div>
        <div>кв.№ {N}</div>           <!-- apartment_number -->
      </div>
      <div ... data-price-pop="..." data-floor-pop="..." data-square-pop="...">
    </div>
    <div class="appartment-filter__room-tabs">
      <span>{Студия | N-комн.}</span>
      <span>{XX.XX м²}</span>
      <span>{N из M этаж}</span>
    </div>
    <div class="appartment-filter__room-price_block">
      <div class="appartment-filter__room-price_current">{Цена} ₽</div>
    </div>
    <div class="appartment-filter__room-price">
      <div class="appartment-filter__room-price_part">{Цена/м²} ₽ / м²</div>
    </div>
  </a>
"""
from __future__ import annotations

import asyncio
import re
from pathlib import Path

import httpx

from parsers.apartments_base import BaseApartmentParser, ApartmentItem, logger

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"


class AkBarsDomApartmentParser(BaseApartmentParser):
    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "akbarsdom.yaml"
        super().__init__(config_path)

    async def parse_all(self) -> list[ApartmentItem]:
        items: list[ApartmentItem] = []
        async with httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"},
            follow_redirects=True,
        ) as client:
            for link_info in self.config.get("apartment_links", []):
                url = link_info["url"]
                city = link_info.get("city", self.config.get("city", "Казань"))
                page_items = await self._parse_all_pages(client, url, city)
                items.extend(page_items)

        self.log.info("Итого %s квартиры: %d", self.site_name, len(items))
        return items

    async def _parse_all_pages(
        self, client: httpx.AsyncClient, base_url: str, city: str
    ) -> list[ApartmentItem]:
        """Пройти все страницы пагинации."""
        all_items: list[ApartmentItem] = []

        page1_html = await self._fetch_page(client, base_url, 1)
        total_pages = self._get_total_pages(page1_html)
        items = self._parse_page_html(page1_html, city)
        all_items.extend(items)
        self.log.info("  Страница 1/%d: %d квартир", total_pages, len(items))

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
        sep = "&" if "?" in base_url else "?"
        url = f"{base_url}{sep}page={page}"
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.text

    def _get_total_pages(self, html: str) -> int:
        pages = re.findall(r'\?page=(\d+)', html)
        if pages:
            return max(int(p) for p in pages)
        return 1

    def _parse_page_html(self, html: str, city: str) -> list[ApartmentItem]:
        """Парсить HTML-страницу и извлечь карточки квартир.

        Карточки: <a href="/apartments/{ID}/" class="appartment-filter__room ...">
        """
        items: list[ApartmentItem] = []

        # Основной паттерн: href перед class
        card_pattern = re.compile(
            r'<a\s+href="/apartments/(\d+)/"[^>]*'
            r'class="appartment-filter__room[^"]*"[^>]*>'
            r'(.*?)</a>',
            re.DOTALL,
        )
        cards = card_pattern.findall(html)

        if not cards:
            # Обратный порядок: class перед href
            card_pattern2 = re.compile(
                r'<a[^>]*class="appartment-filter__room[^"]*"[^>]*'
                r'href="/apartments/(\d+)/"[^>]*>'
                r'(.*?)</a>',
                re.DOTALL,
            )
            cards = card_pattern2.findall(html)

        if not cards:
            # Fallback: любые ссылки на /apartments/ID/
            cards = re.findall(
                r'<a[^>]*href="/apartments/(\d+)/"[^>]*>(.*?)</a>',
                html, re.DOTALL,
            )

        for item_id, card_html in cards:
            item = self._parse_card(item_id, card_html, city)
            if item:
                items.append(item)

        return items

    def _parse_card(
        self, item_id: str, card_html: str, city: str
    ) -> ApartmentItem | None:
        """Разобрать одну карточку квартиры.

        Данные извлекаются из:
        1. <span> тегов внутри табов (тип квартиры, площадь, этаж)
        2. Специфичных div-классов для цены
        3. data-атрибутов как fallback (data-price-pop, data-square-pop)
        4. Div-ов в header-left для ЖК, адреса, номера квартиры
        """
        # ── ЖК (complex_name) ──
        # Первый <div> внутри appartment-filter__room_header-left
        complex_name = ""
        header_left_m = re.search(
            r'appartment-filter__room_header-left">\s*(.*?)</div>\s*</div>',
            card_html, re.DOTALL,
        )
        if header_left_m:
            first_div = re.search(r'<div>([^<]+)</div>', header_left_m.group(1))
            if first_div:
                complex_name = first_div.group(1).strip()

        # ── Адрес / корпус ──
        # <span>{Адрес} •</span> — первый span во втором div header-left
        address = ""
        addr_m = re.search(
            r'appartment-filter__room_header-left">\s*'
            r'(?:<div>[^<]*</div>\s*)?'           # skip first div (complex_name)
            r'<div>\s*<span>([^<]+)</span>',
            card_html, re.DOTALL,
        )
        if addr_m:
            address = addr_m.group(1).strip().rstrip("•").strip()

        # ── Номер квартиры ──
        apt_number = None
        apt_m = re.search(r'кв\.?\s*№?\s*(\d+)', card_html)
        if apt_m:
            apt_number = apt_m.group(1)

        # ── Тип квартиры, площадь, этаж из <span> тегов ──
        spans = [s.strip() for s in re.findall(r'<span[^>]*>([^<]+)</span>', card_html)
                 if s.strip()]

        rooms = 0
        area = 0.0
        floor = 0

        for span_text in spans:
            text_lower = span_text.lower()

            # Тип: "Студия" или "N-комн."
            if "студи" in text_lower:
                rooms = 0
            elif re.match(r'^\d\s*-\s*комн', text_lower):
                m = re.match(r'^(\d)', text_lower)
                if m:
                    rooms = int(m.group(1))

            # Площадь: "24.37 м²"
            if "м²" in span_text:
                area_m = re.search(r'([\d.,]+)\s*м²', span_text)
                if area_m:
                    area = self._parse_float(area_m.group(1))

            # Этаж: "2 из 18 этаж"
            if "этаж" in text_lower:
                floor_m = re.search(r'(\d+)\s+из\s+\d+', span_text)
                if floor_m:
                    floor = int(floor_m.group(1))

        # ── Цена ──
        price = 0.0

        # 1) Из div.appartment-filter__room-price_current
        price_m = re.search(
            r'appartment-filter__room-price_current"[^>]*>(.*?)</div>',
            card_html, re.DOTALL,
        )
        if price_m:
            price = self._parse_float(price_m.group(1))

        # 2) Fallback: data-price-pop
        if not price:
            dprice_m = re.search(r'data-price-pop="([^"]+)"', card_html)
            if dprice_m:
                price = self._parse_float(dprice_m.group(1))

        # ── Площадь fallback из data-square-pop ──
        if not area:
            dsquare_m = re.search(r'data-square-pop="([^"]+)"', card_html)
            if dsquare_m:
                area = self._parse_float(dsquare_m.group(1))

        # ── Валидация ──
        if not price or not area:
            return None

        # ── Корпус ──
        building = self._extract_building(address)

        price_per_meter = round(price / area, 2) if area > 0 else 0

        url = f"{self.config['base_url']}/apartments/{item_id}/"

        return ApartmentItem(
            site=self.site_key,
            city=city,
            complex_name=complex_name,
            building=building,
            item_id=f"apt_{item_id}",
            rooms=rooms,
            floor=floor,
            area=area,
            price=price,
            price_per_meter=price_per_meter,
            url=url,
            apartment_number=apt_number,
        )

    # Маппинг адресов -> корпус (аналогично кладовкам)
    ADDRESS_TO_BUILDING: dict[str, str] = {
        "Назиба Жиганова 2А": "М1/ПК-5",
        "Петра Полушкина 4": "ПК-2",
        "Петра Полушкина 1": "ПК-10",
    }

    def _extract_building(self, address: str) -> str:
        addr_clean = address.strip()
        if addr_clean in self.ADDRESS_TO_BUILDING:
            return self.ADDRESS_TO_BUILDING[addr_clean]

        # Код корпуса: ПК 1-1, УБ-8, М1/ПК-4, М1/1-2
        m = re.search(
            r'((?:М\d*/)?(?:УБ|ПК)[\s\-]?[\d\-]+|М\d+/[\d\-]+)',
            address, re.IGNORECASE,
        )
        if m:
            return m.group(1).strip()

        # Fallback: "корпус N"
        m = re.search(r'корпус\s+(\d+)', address, re.IGNORECASE)
        if m:
            return f"корпус {m.group(1)}"

        if addr_clean:
            self.log.warning("Не удалось извлечь корпус из адреса: '%s'", address)
        return address

    def _extract_rooms(self, card_html: str) -> int:
        """Извлечь количество комнат из карточки."""
        text = card_html.lower()
        if "студи" in text:
            return 0
        m = re.search(r'(\d)\s*[-\s]?\s*комн', text)
        if m:
            return int(m.group(1))
        return 0

    @staticmethod
    def _parse_float(text: str) -> float:
        cleaned = re.sub(r'[^\d.,]', '', text)
        cleaned = cleaned.replace(',', '.')
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    @staticmethod
    def _parse_int(text: str) -> int:
        m = re.search(r'(\d+)', text)
        return int(m.group(1)) if m else 0
