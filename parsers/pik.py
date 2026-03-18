"""
Парсер кладовок ПИК (pik.ru).

Стратегия: извлечение данных из __NEXT_DATA__ (JSON в HTML).
Все кладовки доступны в searchService без необходимости кликать «Показать ещё».
Корпус определяется через regex-поиск объекта "bulk" рядом с каждым flat ID.
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
                self.log.info("Парсинг: %s (%s)", complex_name, url)
                try:
                    items = await self._parse_page(client, url, complex_name, city)
                    all_items.extend(items)
                    self.log.info("  → %d кладовок", len(items))
                except Exception as exc:
                    self.log.error("  ✗ Ошибка при парсинге %s: %s", url, exc)

        self.log.info("Итого ПИК: %d кладовок", len(all_items))
        return all_items

    # ── private ───────────────────────────────────────────

    async def _parse_page(
        self, client: httpx.AsyncClient, url: str, complex_name: str, city: str,
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

        # 2. Получаем список кладовок из chessplan
        chessplan = state["searchService"]["chessplan"]
        flats = chessplan.get("flats", [])
        self.log.debug("  chessplan.flats: %d шт.", len(flats))

        if not flats:
            self.log.warning("  Нет данных в chessplan.flats")
            return []

        # 3. Строим маппинг flat_id → корпус
        #    Рекурсивный обход JSON — надёжнее regex
        flat_to_bulk = self._build_flat_bulk_map(state["searchService"])
        self.log.debug("  Привязано к корпусам: %d из %d", len(flat_to_bulk), len(flats))

        # 4. Собираем StorehouseItem
        base_url = self.config["base_url"].rstrip("/")
        items: list[StorehouseItem] = []

        for flat in flats:
            flat_id = flat["id"]
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

            # Корпус
            bulk_info = flat_to_bulk.get(str(flat_id))
            building = bulk_info[1] if bulk_info else "Не определён"

            # Скидка
            discount_percent = None
            original_price = None
            if old_price and old_price > price:
                original_price = old_price
                discount_percent = round((1 - price / old_price) * 100, 1)
            elif discount and discount > 0:
                discount_percent = discount

            items.append(StorehouseItem(
                site=self.site_key,
                city=city,
                complex_name=complex_name,
                building=building,
                item_id=str(flat_id),
                area=area,
                price=price,
                price_per_meter=meter_price,
                url=item_url,
                item_number=number,
                original_price=original_price,
                discount_percent=discount_percent,
            ))

        # Лог по корпусам
        building_counts = Counter(i.building for i in items)
        for bld, cnt in sorted(building_counts.items()):
            self.log.info("    %s: %d кладовок", bld, cnt)

        return items

    @staticmethod
    def _build_flat_bulk_map(search_state: dict) -> dict[str, tuple[str, str]]:
        """
        Построить маппинг flat_id → (bulk_id, bulk_name).

        Рекурсивно обходит JSON searchService. Когда встречает объект
        с ключом "bulk" (содержащий "id" и "name"), запоминает его
        и привязывает ко всем дочерним объектам с "id".
        """
        result: dict[str, tuple[str, str]] = {}

        def walk(obj, current_bulk=None):
            if isinstance(obj, dict):
                # Если у объекта есть "bulk" — обновляем текущий корпус
                bulk = obj.get("bulk")
                if isinstance(bulk, dict) and "name" in bulk and "id" in bulk:
                    current_bulk = bulk

                # Если у объекта есть "id" и известен корпус — сохраняем
                flat_id = obj.get("id")
                if flat_id is not None and current_bulk:
                    fid = str(flat_id)
                    if fid not in result:
                        result[fid] = (
                            str(current_bulk["id"]),
                            current_bulk["name"],
                        )

                for v in obj.values():
                    walk(v, current_bulk)

            elif isinstance(obj, list):
                for item in obj:
                    walk(item, current_bulk)

        walk(search_state)
        return result


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
        print(f"\n  {cname} / {bld}: {len(group)} шт.")
        for item in sorted(group, key=lambda x: int(x.item_number or 0))[:3]:
            print(f"    №{item.item_number or '?'}: {item.area} м² | "
                  f"{item.price:,.0f} ₽ | {item.price_per_meter:,.0f} ₽/м²")
        if len(group) > 3:
            print(f"    ... и ещё {len(group) - 3}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
