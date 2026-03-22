"""
Парсер квартир с наш.дом.рф (ЕИСЖС).

Стратегия: browser (Playwright) + API.
Сайт защищён ServicePipe — нужен браузер для прохождения JS-challenge.

Квартиры:
  Endpoint: /portal-kn/api/sales/portal/table?externalId={objectId}
  Возвращает все помещения, сгруппированные по подъездам и этажам.
  Фильтруем по type: «Квартира» и «Квартира-студия».

Информация о доме (шапка):
  Парсится со страницы объекта: дата ввода, выдача ключей,
  распроданность, средняя цена за 1 м².

  Также используется API:
  /сервисы/api/object/{objectId}/sales_agg — агрегированные данные продаж
"""
from __future__ import annotations

import asyncio
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from parsers.apartments_base import BaseApartmentParser, ApartmentItem

CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"


@dataclass
class ObjectInfo:
    """Информация о доме со страницы дом.рф."""
    object_id: int
    complex_name: str
    developer: str
    commissioning: str = ""       # Ввод в эксплуатацию (напр. «IV кв. 2026»)
    keys_date: str = ""           # Выдача ключей (напр. «31.03.2027»)
    sold_percent: str = ""        # Распроданность (напр. «64%»)
    avg_price_per_meter: str = "" # Средняя цена за 1 м² (напр. «265 618 ₽»)
    total_apartments: int = 0
    sold_apartments: int = 0


class DomRfApartmentParser(BaseApartmentParser):
    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = CONFIGS_DIR / "domrf_apartments.yaml"
        super().__init__(config_path)

    async def parse_all(self) -> tuple[list[ApartmentItem], list[ObjectInfo]]:
        from playwright.async_api import async_playwright

        items: list[ApartmentItem] = []
        object_infos: list[ObjectInfo] = []
        api_cfg = self.config.get("api", {})
        base_url = self.config["base_url"]
        target_types = set(self.config.get("target_types", ["Квартира", "Квартира-студия"]))

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
                building_override = link_info.get("building", "")

                obj_url = f"{base_url}/сервисы/каталог-новостроек/объект/{object_id}"
                self.log.info("Загрузка страницы %s ...", obj_url)
                await page.goto(obj_url, timeout=60000, wait_until="domcontentloaded")
                await page.wait_for_timeout(8000)

                # Парсим информацию о доме со страницы
                obj_info = await self._parse_header_info(
                    page, base_url, object_id, complex_name, developer,
                )
                object_infos.append(obj_info)

                # Парсим квартиры через API
                object_items = await self._parse_object_apartments(
                    page, base_url, api_cfg, object_id,
                    complex_name, developer, city, target_types,
                    building_override=building_override,
                )
                items.extend(object_items)
                await asyncio.sleep(1)

            await browser.close()

        self.log.info("Итого %s квартиры: %d", self.site_name, len(items))
        return items, object_infos

    async def _parse_header_info(
        self, page, base_url: str, object_id: int,
        complex_name: str, developer: str,
    ) -> ObjectInfo:
        """Спарсить информацию о доме со страницы (ввод, ключи, цена/м², распроданность)."""
        info = ObjectInfo(
            object_id=object_id,
            complex_name=complex_name,
            developer=developer,
        )

        try:
            text = await page.evaluate("() => document.body.innerText.substring(0, 10000)")
            lines = [l.strip() for l in text.split("\n") if l.strip()]

            def _clean_val(val: str) -> str:
                """Очистка дефисов — на дом.рф '-' означает 'нет данных'."""
                return "" if val.strip() in ("-", "–", "—") else val

            # Берём только первое вхождение каждого поля (шапка страницы)
            found = set()
            for i, line in enumerate(lines):
                ll = line.lower()
                if "ввод в эксплуатацию" == ll and "commissioning" not in found:
                    found.add("commissioning")
                    if i + 1 < len(lines):
                        info.commissioning = _clean_val(lines[i + 1])
                elif "выдача ключей" == ll and "keys" not in found:
                    found.add("keys")
                    if i + 1 < len(lines):
                        info.keys_date = _clean_val(lines[i + 1])
                elif ll == "средняя цена за 1 м²" and "avg_price" not in found:
                    found.add("avg_price")
                    if i + 1 < len(lines):
                        info.avg_price_per_meter = _clean_val(lines[i + 1])
                elif "распроданность квартир" == ll and "sold" not in found:
                    found.add("sold")
                    if i + 1 < len(lines):
                        info.sold_percent = _clean_val(lines[i + 1])

            # sales_agg API
            agg_url = f"{base_url}/сервисы/api/object/{object_id}/sales_agg"
            agg = await page.evaluate(
                """async (url) => {
                    const resp = await fetch(url);
                    if (!resp.ok) return null;
                    return await resp.json();
                }""",
                agg_url,
            )
            if agg and isinstance(agg, dict) and "data" in agg:
                apt_data = agg["data"].get("apartmentsAggData", {})
                info.total_apartments = apt_data.get("total", 0)
                info.sold_apartments = apt_data.get("realised", 0)
                if not info.sold_percent and apt_data.get("perc"):
                    info.sold_percent = f"{apt_data['perc']}%"

            # object API — fallback для сданных домов (нет данных на странице)
            obj_url = f"{base_url}/сервисы/api/object/{object_id}"
            obj_data = await page.evaluate(
                """async (url) => {
                    const resp = await fetch(url);
                    if (!resp.ok) return null;
                    return await resp.json();
                }""",
                obj_url,
            )
            if obj_data and isinstance(obj_data, dict) and "data" in obj_data:
                od = obj_data["data"]
                if not info.avg_price_per_meter and od.get("objPriceAvg"):
                    info.avg_price_per_meter = f"{int(od['objPriceAvg']):,} ₽".replace(",", " ")
                if not info.sold_percent and od.get("soldOutPerc"):
                    info.sold_percent = f"{int(od['soldOutPerc'] * 100)}%"
                if not info.total_apartments and od.get("objElemLivingCnt"):
                    info.total_apartments = od["objElemLivingCnt"]

        except Exception as e:
            self.log.warning("Ошибка парсинга header для %d: %s", object_id, e)

        self.log.info(
            "  Объект %d: ввод=%s, ключи=%s, продано=%s, цена/м²=%s",
            object_id, info.commissioning, info.keys_date,
            info.sold_percent, info.avg_price_per_meter,
        )
        return info

    async def _parse_object_apartments(
        self,
        page,
        base_url: str,
        api_cfg: dict,
        object_id: int,
        complex_name: str,
        developer: str,
        city: str,
        target_types: set[str],
        building_override: str = "",
    ) -> list[ApartmentItem]:
        """Загрузить все квартиры одного объекта через table API."""
        table_ep = api_cfg.get(
            "table_endpoint",
            "/portal-kn/api/sales/portal/table?externalId={object_id}",
        ).format(object_id=object_id)

        table_url = f"{base_url}{table_ep}"
        self.log.debug("fetch %s", table_url)

        raw = await page.evaluate(
            """async (url) => {
                const resp = await fetch(url);
                if (!resp.ok) return {error: resp.status, text: await resp.text()};
                return await resp.json();
            }""",
            table_url,
        )

        if isinstance(raw, dict) and "error" in raw:
            self.log.error(
                "API вернул %s для объекта %d: %s",
                raw["error"], object_id, str(raw.get("text", ""))[:200],
            )
            return []

        entrances = raw.get("entrances", [])
        if not entrances:
            self.log.info("  %s (объект %d): нет данных о квартирах", complex_name, object_id)
            return []

        items: list[ApartmentItem] = []
        skipped: Counter = Counter()

        for entrance in entrances:
            entrance_num = entrance.get("entranceNumber", 0)
            for floor in entrance.get("floors", []):
                floor_num = floor.get("floorNumber", 0)
                for flat in floor.get("flats", []):
                    flat_type = (flat.get("type") or "").strip()
                    if flat_type not in target_types:
                        skipped[flat_type] += 1
                        continue

                    item = self._parse_flat(
                        flat, object_id, complex_name, developer, city,
                        base_url, entrance_num, floor_num,
                        building_override=building_override,
                    )
                    if item:
                        items.append(item)

        for ftype, cnt in sorted(skipped.items()):
            self.log.debug("    Пропущено: %s — %d шт.", ftype, cnt)

        # Batch fetch livingArea for all items
        await self._fetch_living_areas(page, base_url, items)

        rooms_counts = Counter(it.rooms for it in items)
        for rooms, cnt in sorted(rooms_counts.items()):
            label = "Студия" if rooms == 0 else f"{rooms}-комн."
            self.log.info("    %s: %d шт.", label, cnt)

        self.log.info(
            "  %s (объект %d): отобрано %d квартир",
            complex_name, object_id, len(items),
        )
        return items

    async def _fetch_living_areas(
        self, page, base_url: str, items: list[ApartmentItem],
        batch_size: int = 50,
    ) -> None:
        """Batch fetch livingArea for items via /portal-kn/api/sales/portal/flat/{elemId}."""
        # Collect items with valid elemIds
        items_with_ids = [(i, it) for i, it in enumerate(items)
                          if it.item_id and not it.item_id.startswith("domrf_")]
        if not items_with_ids:
            return

        self.log.info("    Загрузка жилой площади для %d квартир...", len(items_with_ids))

        for batch_start in range(0, len(items_with_ids), batch_size):
            batch = items_with_ids[batch_start:batch_start + batch_size]
            elem_ids = [it.item_id for _, it in batch]

            results = await page.evaluate(
                """async (data) => {
                    const {baseUrl, elemIds} = data;
                    const results = await Promise.all(
                        elemIds.map(async (id) => {
                            try {
                                const resp = await fetch(
                                    baseUrl + '/portal-kn/api/sales/portal/flat/' + id
                                );
                                if (!resp.ok) return {id, livingArea: null};
                                const json = await resp.json();
                                return {id, livingArea: json.livingArea || null};
                            } catch (e) {
                                return {id, livingArea: null};
                            }
                        })
                    );
                    return results;
                }""",
                {"baseUrl": base_url, "elemIds": elem_ids},
            )

            # Map results back to items
            la_map = {r["id"]: r["livingArea"] for r in results if r.get("livingArea")}
            for idx, item in batch:
                la = la_map.get(item.item_id)
                if la is not None:
                    try:
                        items[idx].living_area = float(la)
                    except (ValueError, TypeError):
                        pass

        filled = sum(1 for it in items if it.living_area)
        self.log.info("    Жилая площадь получена для %d из %d квартир", filled, len(items))

    def _parse_flat(
        self,
        flat: dict,
        object_id: int,
        complex_name: str,
        developer: str,
        city: str,
        base_url: str,
        entrance_num: int,
        floor_num: int,
        building_override: str = "",
    ) -> ApartmentItem | None:
        """Преобразовать одну квартиру из API в ApartmentItem."""
        try:
            elem_id = flat.get("elemId", "")
            ods_id = flat.get("odsId", "")
            area = float(flat.get("totalArea") or 0)
            flat_type = (flat.get("type") or "").strip()

            # Комнатность: студия = 0, иначе из поля rooms
            if flat_type == "Квартира-студия" or flat.get("isStudio"):
                rooms = 0
            else:
                rooms = int(flat.get("rooms") or 0)

            # Цены на дом.рф отсутствуют
            price = 0
            price_per_meter = 0

            # Корпус: приоритет — из конфига (docx маппинг), fallback — из odsId
            apartment_number = ""
            if ods_id:
                parts = ods_id.split("/")
                if len(parts) >= 3:
                    apartment_number = parts[2]

            building = building_override if building_override else ""
            if not building and ods_id:
                parts = ods_id.split("/")
                if len(parts) >= 2:
                    building = parts[1]

            # Подъезд → в примечание к корпусу через ||
            if entrance_num:
                building = f"{building}||подъезд {entrance_num}"

            item_id = elem_id or f"domrf_{object_id}_{ods_id}"
            url = f"{base_url}/сервисы/каталог-квартир/квартира/{elem_id}"

            return ApartmentItem(
                site=self.site_key,
                city=city,
                complex_name=complex_name,
                building=building,
                item_id=item_id,
                rooms=rooms,
                floor=floor_num,
                area=area,
                price=price,
                price_per_meter=price_per_meter,
                url=url,
                apartment_number=apartment_number,
                developer=developer,
            )
        except (ValueError, KeyError, TypeError) as e:
            self.log.warning("Ошибка парсинга квартиры: %s — %s", e, flat)
            return None
