"""
Парсер кладовок ПИК (pik.ru).

Стратегия: извлечение данных из __NEXT_DATA__ (JSON в HTML).
Все кладовки доступны в filteredChessplan.data.bulks без «Показать ещё».
Секция определяется через структуру bulks → sections → floors → flats.
Корпус 1 Сибирово делится на 1.1 (секции 1-4) и 1.2 (секции 5+) через конфиг.
"""
from __future__ import annotations

import asyncio
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import httpx

from parsers.base import BaseParser, StorehouseItem, logger


class PikParser(BaseParser):
    """Парсер кладовок для сайта pik.ru."""

    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:
            config_path = Path(__file__).resolve().parent.parent / "configs" / "pik.yaml"
        super().__init__(config_path)

    # ── public ────────────────────────────────────────────

    async def parse_all(self) -> list[StorehouseItem]:
        """Спарсить все ссылки из конфига."""
        all_items: list[StorehouseItem] = []
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
            for link_cfg in self.config["links"]:
                url = link_cfg["url"]
                complex_name = link_cfg["complex_name"]
                city = link_cfg.get("city", "")
                building_split = link_cfg.get("building_split", {})
                self.log.info("Парсинг: %s (%s)", complex_name, url)
                try:
                    items = await self._parse_page(
                        client, url, complex_name, city, building_split
                    )
                    all_items.extend(items)
                    self.log.info("  → %d кладовок", len(items))
                except Exception as exc:
                    self.log.error("  ✗ Ошибка при парсинге %s: %s", url, exc)

        self.log.info("Итого ПИК: %d кладовок", len(all_items))
        return all_items

    # ── private ───────────────────────────────────────────

    async def _parse_page(
        self,
        client: httpx.AsyncClient,
        url: str,
        complex_name: str,
        city: str,
        building_split: dict,
    ) -> list[StorehouseItem]:
        """Загрузить HTML, извлечь __NEXT_DATA__, спарсить кладовки."""
        resp = await client.get(url)
        resp.raise_for_status()
        html = resp.text

        # 1. Извлекаем __NEXT_DATA__
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
        )
        if not m:
            raise ValueError("__NEXT_DATA__ не найден на странице")

        next_data = json.loads(m.group(1))
        state = next_data["props"]["pageProps"]["initialState"]
        search = state["searchService"]

        # 2. Строим маппинг flat_id → (bulk_name, section_name, section_number)
        #    через filteredChessplan.data.bulks → sections → floors → flats
        flat_map = self._build_flat_map(search)
        self.log.debug("  Привязано к секциям: %d", len(flat_map))

        # 3. Получаем список кладовок из chessplan.flats (полные данные)
        flats = search["chessplan"].get("flats", [])
        self.log.debug("  chessplan.flats: %d шт.", len(flats))

        if not flats:
            self.log.warning("  Нет данных в chessplan.flats")
            return []

        # 4. Собираем StorehouseItem
        base_url = self.config["base_url"].rstrip("/")
        items: list[StorehouseItem] = []

        for flat in flats:
            flat_id = str(flat["id"])
            status = flat.get("status", "")

            # Пропускаем проданные/забронированные
            if status not in ("free",):
                continue

            area = flat.get("area", 0)
            price = flat.get("price", 0)
            old_price = flat.get("oldPrice")
            discount = flat.get("discount", 0)
            meter_price = flat.get("meterPrice", 0)
            number = str(flat.get("number", "")) or None
            item_url = f"{base_url}/storehouse/{flat_id}"

            # Корпус + секция из маппинга
            info = flat_map.get(flat_id)
            if info:
                bulk_name = info["bulk_name"]
                section_name = info["section_name"]
                section_number = info["section_number"]
            else:
                bulk_name = "Не определён"
                section_name = None
                section_number = None

            # Разделение корпуса по конфигу (например, Корпус 1 → 1.1 / 1.2)
            building = self._resolve_building(
                bulk_name, section_number, building_split
            )

            # Скидка
            discount_percent = None
            original_price = None
            if old_price and old_price > price:
                original_price = old_price
                discount_percent = round((1 - price / old_price) * 100, 1)
            elif discount and discount > 0:
                discount_percent = discount

            item = StorehouseItem(
                site=self.site_key,
                city=city,
                complex_name=complex_name,
                building=building,
                item_id=flat_id,
                area=area,
                price=price,
                price_per_meter=meter_price,
                url=item_url,
                item_number=number,
                original_price=original_price,
                discount_percent=discount_percent,
            )
            # Сохраняем секцию для примечания
            item._section_name = section_name
            items.append(item)

        # Лог по корпусам
        building_counts = Counter(i.building for i in items)
        for bld, cnt in sorted(building_counts.items()):
            self.log.info("    %s: %d кладовок", bld, cnt)

        return items

    @staticmethod
    def _build_flat_map(search_state: dict) -> dict[str, dict]:
        """
        Построить маппинг flat_id → {bulk_name, section_name, section_number}
        через filteredChessplan.data.bulks → sections → floors → flats.
        """
        result: dict[str, dict] = {}

        fc = search_state.get("filteredChessplan", {})
        data = fc.get("data", {})
        bulks = data.get("bulks", [])

        for bulk in bulks:
            if not isinstance(bulk, dict):
                continue
            bulk_name = bulk.get("name", "")
            sections = bulk.get("sections", [])

            for section in sections:
                if not isinstance(section, dict):
                    continue
                section_name = section.get("name", "")
                section_number = section.get("number")

                floors = section.get("floors", {})
                # floors может быть dict (ключ — номер этажа) или list
                floor_list = (
                    floors.values() if isinstance(floors, dict)
                    else floors if isinstance(floors, list)
                    else []
                )

                for floor in floor_list:
                    if not isinstance(floor, dict):
                        continue
                    for flat in floor.get("flats", []):
                        if isinstance(flat, dict) and "id" in flat:
                            fid = str(flat["id"])
                            if fid not in result:
                                result[fid] = {
                                    "bulk_name": bulk_name,
                                    "section_name": section_name,
                                    "section_number": section_number,
                                }

        return result

    @staticmethod
    def _resolve_building(
        bulk_name: str,
        section_number: int | None,
        building_split: dict,
    ) -> str:
        """
        Применить разделение корпуса из конфига.

        Пример building_split:
          "Корпус 1":
            - sub_name: "Корпус 1.1"
              sections: [1, 2, 3, 4]
            - sub_name: "Корпус 1.2"
              sections: [5, 6, 7]
        """
        if not building_split or bulk_name not in building_split:
            return bulk_name

        if section_number is None:
            return bulk_name

        for rule in building_split[bulk_name]:
            if section_number in rule["sections"]:
                return rule["sub_name"]

        return bulk_name


# ── Точка входа для отдельного запуска ────────────────────

async def main():
    """Запуск парсера ПИК отдельно."""
    parser = PikParser()
    items = await parser.parse_all()

    # Группируем по ЖК/корпусу
    groups: dict[tuple, list] = defaultdict(list)
    for item in items:
        groups[(item.complex_name, item.building)].append(item)

    print(f"\n{'='*60}")
    print(f"Найдено кладовок: {len(items)}")
    print(f"{'='*60}")
    for (cname, bld), group in sorted(groups.items()):
        # Показать секции
        sections = set()
        for it in group:
            sec = getattr(it, '_section_name', None)
            if sec:
                sections.add(sec)
        sec_info = f" ({', '.join(sorted(sections))})" if sections else ""

        print(f"\n  {cname} / {bld}: {len(group)} шт.{sec_info}")
        for item in sorted(group, key=lambda x: int(x.item_number or 0))[:3]:
            print(f"    №{item.item_number or '?'}: {item.area} м² | "
                  f"{item.price:,.0f} ₽ | {item.price_per_meter:,.0f} ₽/м²")
        if len(group) > 3:
            print(f"    ... и ещё {len(group) - 3}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
