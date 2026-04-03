"""
Парсер квартир ПИК (pik.ru).

Стратегия двухэтапная:
  1. Загрузить __NEXT_DATA__ для получения blockId, locationId, building_split
  2. Загрузить все квартиры через API filter.dev-service.tech с пагинацией

API: https://filter.dev-service.tech/api/v1/filter/flat-by-block/{blockId}
  Параметры: type=1,2 (квартиры), location={locationId}, flatPage, flatLimit, onlyFlats=1
  Отдаёт макс. 20 квартир за запрос.

rooms: -1 = студия → мы конвертируем в 0
"""
from __future__ import annotations

import asyncio
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path

import httpx

from parsers.apartments_base import BaseApartmentParser, ApartmentItem, rooms_label, logger

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"

# API для загрузки квартир ПИК
FILTER_API_BASE = "https://filter.dev-service.tech/api/v1/filter"
FLAT_PER_PAGE = 20


class PikApartmentParser(BaseApartmentParser):
    """Парсер квартир для сайта pik.ru."""

    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "pik.yaml"
        super().__init__(config_path)

    async def parse_all(self) -> list[ApartmentItem]:
        """Спарсить все квартиры из конфига."""
        all_items: list[ApartmentItem] = []
        async with httpx.AsyncClient(
            timeout=30,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ru-RU,ru;q=0.9",
            },
        ) as client:
            for link_cfg in self.config.get("apartment_links", []):
                url = link_cfg["url"]
                complex_name = link_cfg["complex_name"]
                city = link_cfg.get("city", "")
                building_split = link_cfg.get("building_split", {})
                self.log.info("Парсинг квартир: %s (%s)", complex_name, url)
                try:
                    items = await self._parse_project(
                        client, url, complex_name, city, building_split
                    )
                    all_items.extend(items)
                    self.log.info("  -> %d квартир", len(items))
                except Exception as exc:
                    self.log.error("  Ошибка при парсинге %s: %s", url, exc, exc_info=True)

        self.log.info("Итого ПИК квартиры: %d", len(all_items))
        return all_items

    async def _parse_project(
        self,
        client: httpx.AsyncClient,
        url: str,
        complex_name: str,
        city: str,
        building_split: dict,
    ) -> list[ApartmentItem]:
        """
        1. Загрузить __NEXT_DATA__ для blockId и locationId
        2. Загрузить все квартиры через API с пагинацией
        """
        # Шаг 1: __NEXT_DATA__ для метаданных
        resp = await client.get(url)
        resp.raise_for_status()
        html = resp.text

        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
        )
        if not m:
            raise ValueError("__NEXT_DATA__ не найден на странице")

        try:
            next_data = json.loads(m.group(1))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Не удалось распарсить __NEXT_DATA__ как JSON: {exc}"
            ) from exc

        try:
            state = next_data["props"]["pageProps"]["initialState"]
        except KeyError as exc:
            raise ValueError(
                f"Структура __NEXT_DATA__ изменилась — не найден ключ {exc}. "
                "Возможно, ПИК обновил сайт."
            ) from exc
        filter_svc = state.get("filterService", {})

        # Извлекаем blockId и locationId из filterService
        input_values = filter_svc.get("inputValues", {})
        block_id = input_values.get("block")
        location_id = input_values.get("location")

        if not block_id:
            # Попробуем из staticFilter
            static_filter = filter_svc.get("staticFilter", {})
            sf_data = static_filter.get("data", {})
            block_info = sf_data.get("block", {})
            block_id = block_info.get("id")

        self.log.info("  blockId=%s, locationId=%s", block_id, location_id)

        if not block_id:
            raise ValueError(f"Не удалось определить blockId для {url}")

        # Шаг 2: API с пагинацией
        base_url = self.config["base_url"].rstrip("/")
        all_items: dict[str, ApartmentItem] = {}

        # Первый запрос — определяем total
        api_url = (
            f"{FILTER_API_BASE}/flat-by-block/{block_id}"
            f"?type=1,2&location={location_id}&flatPage=1"
            f"&flatLimit={FLAT_PER_PAGE}&onlyFlats=1"
        )
        first_resp = await client.get(api_url)
        first_resp.raise_for_status()
        first_data = first_resp.json()["data"]
        total = first_data["stats"]["count"]
        total_pages = math.ceil(total / FLAT_PER_PAGE)
        self.log.info("  Всего квартир: %d (%d страниц)", total, total_pages)

        # Обрабатываем первую страницу
        for flat in first_data.get("items", []):
            item = self._parse_flat(flat, complex_name, city, building_split, base_url)
            if item:
                all_items[item.item_id] = item

        # Остальные страницы — пачками по 5 для скорости
        batch_size = 5
        for batch_start in range(2, total_pages + 1, batch_size):
            batch_end = min(batch_start + batch_size, total_pages + 1)
            tasks = []
            for page in range(batch_start, batch_end):
                page_url = (
                    f"{FILTER_API_BASE}/flat-by-block/{block_id}"
                    f"?type=1,2&location={location_id}&flatPage={page}"
                    f"&flatLimit={FLAT_PER_PAGE}&onlyFlats=1"
                )
                tasks.append(client.get(page_url))

            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for i, resp_or_exc in enumerate(responses):
                page_num = batch_start + i
                if isinstance(resp_or_exc, Exception):
                    self.log.error("  Ошибка на странице %d: %s", page_num, resp_or_exc)
                    continue
                try:
                    page_data = resp_or_exc.json()["data"]
                    for flat in page_data.get("items", []):
                        item = self._parse_flat(flat, complex_name, city, building_split, base_url)
                        if item:
                            all_items[item.item_id] = item
                except Exception as e:
                    self.log.error("  Ошибка парсинга страницы %d: %s", page_num, e)

            self.log.debug("  Страницы %d-%d: загружено, итого %d",
                           batch_start, batch_end - 1, len(all_items))

        items = list(all_items.values())

        # Лог по типам
        rooms_counts = Counter(i.rooms for i in items)
        for r, cnt in sorted(rooms_counts.items()):
            self.log.info("    %s: %d квартир", rooms_label(r), cnt)

        return items

    def _parse_flat(
        self,
        flat: dict,
        complex_name: str,
        city: str,
        building_split: dict,
        base_url: str,
    ) -> ApartmentItem | None:
        """Преобразовать один элемент из API в ApartmentItem."""
        try:
            flat_id = str(flat["id"])
            status = flat.get("status", "")
            if status not in ("free",):
                return None

            area = float(flat.get("area", 0))
            price = float(flat.get("price", 0))
            meter_price = float(flat.get("meterPrice", 0))

            if price <= 0 or area <= 0:
                return None

            # rooms: -1 = студия → 0
            rooms_raw = flat.get("rooms", 0)
            rooms = 0 if rooms_raw == -1 else int(rooms_raw)

            floor_num = int(flat.get("floor", 0))
            number = str(flat.get("number", "")) or None

            # Корпус
            bulk_name = flat.get("bulkName") or ""
            if not bulk_name:
                bulk_info = flat.get("bulk", {})
                if isinstance(bulk_info, dict):
                    bulk_name = bulk_info.get("name", "")

            section_number = flat.get("sectionNumber")
            building = self._resolve_building(bulk_name, section_number, building_split)

            # Секция для примечания
            section_info = flat.get("section", {})
            section_name = None
            if isinstance(section_info, dict):
                section_name = section_info.get("name")

            # Скидка
            old_price = flat.get("oldPrice")
            discount = flat.get("discount", 0)
            discount_percent = None
            original_price = None
            if old_price and old_price > price:
                original_price = float(old_price)
                discount_percent = round((1 - price / original_price) * 100, 1)
            elif discount and discount > 0:
                discount_percent = float(discount)

            # URL
            href = flat.get("href") or f"/flat/{flat_id}"
            item_url = f"{base_url}{href}" if href.startswith("/") else href

            if not meter_price and area > 0:
                meter_price = round(price / area, 2)

            item = ApartmentItem(
                site=self.site_key,
                city=city,
                complex_name=complex_name,
                building=building,
                item_id=flat_id,
                rooms=rooms,
                floor=floor_num,
                area=area,
                price=price,
                price_per_meter=meter_price,
                url=item_url,
                apartment_number=number,
                original_price=original_price,
                discount_percent=discount_percent,
            )
            item._section_name = section_name
            return item

        except (ValueError, KeyError) as e:
            self.log.warning("Ошибка парсинга квартиры: %s", e)
            return None

    @staticmethod
    def _resolve_building(
        bulk_name: str,
        section_number: int | None,
        building_split: dict,
    ) -> str:
        """Применить разделение корпуса из конфига."""
        if not building_split or bulk_name not in building_split:
            return bulk_name

        if section_number is None:
            return bulk_name

        for rule in building_split[bulk_name]:
            if section_number in rule["sections"]:
                return rule["sub_name"]

        return bulk_name


async def main():
    parser = PikApartmentParser()
    items = await parser.parse_all()

    groups: dict[tuple, list] = defaultdict(list)
    for item in items:
        groups[(item.complex_name, item.building, item.rooms_label)].append(item)

    print(f"\n{'='*60}")
    print(f"Найдено квартир: {len(items)}")
    print(f"{'='*60}")
    for (cname, bld, rlabel), group in sorted(groups.items()):
        prices = [it.price for it in group]
        avg_p = sum(prices) / len(prices)
        print(f"  {cname} / {bld} / {rlabel}: {len(group)} шт. "
              f"(ср. {avg_p:,.0f} р)")
    print()


if __name__ == "__main__":
    asyncio.run(main())
