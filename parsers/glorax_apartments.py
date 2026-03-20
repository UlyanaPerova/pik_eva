"""
Парсер квартир GloraX (glorax.com).

Данные загружаются через CMS API (glorax-api-dev.city-digital.ru).
Тип: flat (вместо storage для кладовок).
Используем тот же обход пагинации — по секциям + ценовым диапазонам.
"""
from __future__ import annotations

import asyncio
import re
from collections import Counter
from pathlib import Path

import httpx

from parsers.apartments_base import BaseApartmentParser, ApartmentItem, logger

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"


class GloraxApartmentParser(BaseApartmentParser):
    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "glorax.yaml"
        super().__init__(config_path)

    async def parse_all(self) -> list[ApartmentItem]:
        items: list[ApartmentItem] = []
        api_cfg = self.config.get("api", {})
        api_base = api_cfg.get("base_url", "https://glorax-api-dev.city-digital.ru/api/v1")
        # Для квартир используем /filter/lots (не lots-applied!)
        lots_ep = "/filter/lots"

        async with httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"},
            follow_redirects=True,
        ) as client:
            for link_info in self.config.get("apartment_links", []):
                city = link_info.get("city", "Казань")
                page_items = await self._parse_all_flats(
                    client, api_base, lots_ep, city
                )
                items.extend(page_items)

        self.log.info("Итого %s квартиры: %d", self.site_name, len(items))
        return items

    async def _parse_all_flats(
        self,
        client: httpx.AsyncClient,
        api_base: str,
        lots_ep: str,
        city: str,
    ) -> list[ApartmentItem]:
        """Загрузить все квартиры по проектам из конфига кладовок."""
        all_items: dict[str, ApartmentItem] = {}

        # Берём проекты из конфига кладовок (там есть project slug'и)
        projects = self.config.get("links", [])
        for proj_info in projects:
            project = proj_info.get("project", "")
            complex_name = proj_info.get("complex_name", "")
            if not project:
                continue

            base_params = f"?filter[type]=flat&filter[project]={project}&filter[withReserved]=false"
            url = f"{api_base}{lots_ep}{base_params}"
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

            section_ids = data.get("filter", {}).get("section", [])
            total = data.get("pagination", {}).get("total", 0)
            self.log.info("  %s: %d квартир, %d секций", complex_name, total, len(section_ids))

            for sid in section_ids:
                section_items = await self._parse_section(
                    client, api_base, lots_ep, base_params, sid, city
                )
                for item in section_items:
                    all_items[item.item_id] = item
                await asyncio.sleep(0.2)

        items = list(all_items.values())

        # Лог по ЖК и типам
        jk_counts = Counter((it.complex_name, it.rooms_label) for it in items)
        for (jk, rl), cnt in sorted(jk_counts.items()):
            self.log.info("    %s / %s: %d квартир", jk, rl, cnt)

        return items

    async def _parse_section(
        self,
        client: httpx.AsyncClient,
        api_base: str,
        lots_ep: str,
        base_params: str,
        section_id: int,
        city: str,
    ) -> list[ApartmentItem]:
        """Загрузить все квартиры одной секции."""
        url = f"{api_base}{lots_ep}{base_params}&filter[section]={section_id}"
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

        lots = data.get("data", [])
        total = data.get("pagination", {}).get("total", 0)
        filter_info = data.get("filter", {})

        items: dict[str, ApartmentItem] = {}
        for lot in lots:
            item = self._parse_lot(lot, city)
            if item:
                items[item.item_id] = item

        # Разбиваем по ценам если не все получены
        if total > len(items):
            price_min = filter_info.get("priceMin", 0)
            price_max = filter_info.get("priceMax", 999999999)

            step = (price_max - price_min) / 4
            for i in range(4):
                pmin = int(price_min + i * step)
                pmax = int(price_min + (i + 1) * step)
                url2 = (f"{api_base}{lots_ep}{base_params}"
                        f"&filter[section]={section_id}"
                        f"&filter[priceMin]={pmin}&filter[priceMax]={pmax}")
                resp2 = await client.get(url2)
                for lot in resp2.json().get("data", []):
                    item = self._parse_lot(lot, city)
                    if item:
                        items[item.item_id] = item
                await asyncio.sleep(0.1)

            # 8 частей если нужно
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
                        item = self._parse_lot(lot, city)
                        if item:
                            items[item.item_id] = item
                    await asyncio.sleep(0.1)

        return list(items.values())

    def _parse_lot(self, lot: dict, city: str) -> ApartmentItem | None:
        """Преобразовать один лот из API в ApartmentItem."""
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

            # Комнаты (rooms=0 — студия, None — тоже считаем студией)
            rooms_raw = lot.get("rooms")
            rooms = int(rooms_raw) if rooms_raw is not None else 0

            # Этаж
            floor = int(lot.get("floor", 0))

            # ЖК
            project_name = lot.get("projectName", "")
            complex_name = project_name if project_name else lot.get("project", "")

            # Корпус + секция
            section = lot.get("section", "")
            building_raw = lot.get("building", "")
            building = re.sub(r'\s*секции?\s*\d+[-–]\d+', '', building_raw).strip()
            if section:
                building = f"{building}||секция {section}"

            room_num = lot.get("roomNum", "")
            url = f"https://glorax.com/object/{item_id}"

            return ApartmentItem(
                site=self.site_key,
                city=city,
                complex_name=complex_name,
                building=building,
                item_id=item_id,
                rooms=rooms,
                floor=floor,
                area=area,
                price=price,
                price_per_meter=price_per_meter,
                url=url,
                apartment_number=room_num,
                original_price=original_price,
                discount_percent=discount_percent,
            )
        except (ValueError, KeyError) as e:
            self.log.warning("Ошибка парсинга лота: %s", e)
            return None
