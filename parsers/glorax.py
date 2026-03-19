"""
Парсер кладовок GloraX (glorax.com).

Данные загружаются через CMS API (glorax-api-dev.city-digital.ru).
API багованый: пагинация не работает (всегда отдаёт первые 15).
Обход: фильтрация по section, затем по ценовому диапазону для секций с >15 лотов.
Скидки: price — без скидки, priceOffer — со скидкой, discount — %.
"""
from __future__ import annotations

import asyncio
from collections import Counter
from pathlib import Path

import httpx

from parsers.base import BaseParser, StorehouseItem, logger

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"


class GloraxParser(BaseParser):
    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "glorax.yaml"
        super().__init__(config_path)

    async def parse_all(self) -> list[StorehouseItem]:
        items: list[StorehouseItem] = []
        api_cfg = self.config.get("api", {})
        api_base = api_cfg.get("base_url", "https://glorax-api-dev.city-digital.ru/api/v1")
        lots_ep = api_cfg.get("lots_endpoint", "/filter/lots-applied")

        async with httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"},
            follow_redirects=True,
        ) as client:
            for link_info in self.config.get("links", []):
                project = link_info["project"]
                complex_name = link_info["complex_name"]
                city = link_info.get("city", "Казань")

                project_items = await self._parse_project(
                    client, api_base, lots_ep, project, complex_name, city
                )
                items.extend(project_items)

        self.log.info("Итого %s: %d кладовок", self.site_name, len(items))
        return items

    async def _parse_project(
        self,
        client: httpx.AsyncClient,
        api_base: str,
        lots_ep: str,
        project: str,
        complex_name: str,
        city: str,
    ) -> list[StorehouseItem]:
        """
        Загрузить все кладовки проекта.
        1. Получаем section IDs из фильтров
        2. По каждой секции: если total <= 15 — один запрос, иначе разбиваем по цене
        """
        base_params = (
            f"?filter[type]=storage"
            f"&filter[project]={project}"
            f"&filter[withReserved]=false"
        )
        url = f"{api_base}{lots_ep}{base_params}"
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

        section_ids = data.get("filter", {}).get("section", [])
        total = data.get("pagination", {}).get("total", 0)
        self.log.info("  %s: %d кладовок, %d секций", complex_name, total, len(section_ids))

        all_items: dict[str, StorehouseItem] = {}  # id → item (дедупликация)

        for sid in section_ids:
            section_items = await self._parse_section(
                client, api_base, lots_ep, base_params, sid, complex_name, city
            )
            for item in section_items:
                all_items[item.item_id] = item
            await asyncio.sleep(0.2)

        items = list(all_items.values())

        # Логирование по секциям
        section_counts = Counter(it.building for it in items)
        for sec, cnt in sorted(section_counts.items()):
            self.log.info("    %s: %d кладовок", sec, cnt)

        return items

    async def _parse_section(
        self,
        client: httpx.AsyncClient,
        api_base: str,
        lots_ep: str,
        base_params: str,
        section_id: int,
        complex_name: str,
        city: str,
    ) -> list[StorehouseItem]:
        """Загрузить все кладовки одной секции. Разбивает по цене если >15."""
        url = f"{api_base}{lots_ep}{base_params}&filter[section]={section_id}"
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

        lots = data.get("data", [])
        total = data.get("pagination", {}).get("total", 0)
        filter_info = data.get("filter", {})

        items: dict[str, StorehouseItem] = {}
        for lot in lots:
            item = self._parse_lot(lot, complex_name, city)
            if item:
                items[item.item_id] = item

        # Если total > 15 — разбиваем по ценовому диапазону
        if total > len(items):
            price_min = filter_info.get("priceMin", 0)
            price_max = filter_info.get("priceMax", 999999999)

            # Разбиваем на 4 части
            step = (price_max - price_min) / 4
            for i in range(4):
                pmin = int(price_min + i * step)
                pmax = int(price_min + (i + 1) * step)
                url2 = (f"{api_base}{lots_ep}{base_params}"
                        f"&filter[section]={section_id}"
                        f"&filter[priceMin]={pmin}&filter[priceMax]={pmax}")
                resp2 = await client.get(url2)
                for lot in resp2.json().get("data", []):
                    item = self._parse_lot(lot, complex_name, city)
                    if item:
                        items[item.item_id] = item
                await asyncio.sleep(0.1)

            # Если всё ещё не хватает — разбиваем на 8
            if len(items) < total:
                step = (price_max - price_min) / 8
                for i in range(8):
                    pmin = int(price_min + i * step)
                    pmax = int(price_min + (i + 1) * step)
                    url3 = (f"{api_base}{lots_ep}{base_params}"
                            f"&filter[section]={section_id}"
                            f"&filter[priceMin]={pmin}&filter[priceMax]={pmax}")
                    resp3 = await client.get(url3)
                    for lot in resp3.json().get("data", []):
                        item = self._parse_lot(lot, complex_name, city)
                        if item:
                            items[item.item_id] = item
                    await asyncio.sleep(0.1)

        return list(items.values())

    def _parse_lot(
        self, lot: dict, complex_name: str, city: str
    ) -> StorehouseItem | None:
        """Преобразовать один лот из API в StorehouseItem."""
        try:
            item_id = str(lot["id"])
            area = float(lot.get("square", 0))
            price_full = float(lot.get("price", 0))
            price_offer = lot.get("priceOffer")
            discount = lot.get("discount")

            if price_offer and float(price_offer) > 0:
                price = float(price_offer)
                original_price = price_full
                discount_percent = float(discount) if discount else None
            else:
                price = price_full
                original_price = None
                discount_percent = None

            if price <= 0 or area <= 0:
                return None

            price_per_meter = round(price / area, 2)

            section = lot.get("section", "")
            building_raw = lot.get("building", "")
            # Убираем "секции X-Y" из имени корпуса: "УБ-1 секции 1-4" → "УБ-1"
            import re as _re
            building = _re.sub(r'\s*секции?\s*\d+[-–]\d+', '', building_raw).strip()
            # Секция → записываем в building через разделитель для примечания
            if section:
                building = f"{building}||секция {section}"

            room_num = lot.get("roomNum", "")
            # URL на конкретную кладовку
            url = f"https://glorax.com/storage/{item_id}"

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
                item_number=room_num,
                original_price=original_price,
                discount_percent=discount_percent,
            )
        except (ValueError, KeyError) as e:
            self.log.warning("Ошибка парсинга лота: %s", e)
            return None
