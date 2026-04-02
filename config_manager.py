"""
Менеджер конфигурации ДОМ.РФ — API для добавления/удаления ссылок.

Используется оркестрантом и будущим плагином управления.

Примеры:
    from config_manager import add_link, remove_link, list_links

    # Добавить новый объект
    add_link(
        object_id=12345,
        complex_name="Новый ЖК",
        building="Корпус 1",
        developer="Ак Барс Дом",
        city="Казань",
    )

    # Удалить объект
    remove_link(object_id=12345)

    # Список всех ссылок
    links = list_links()
"""
from __future__ import annotations

import logging
from copy import deepcopy
from pathlib import Path

import yaml

logger = logging.getLogger("config_manager")

CONFIGS_DIR = Path(__file__).resolve().parent / "configs"
DOMRF_APARTMENTS_PATH = CONFIGS_DIR / "domrf_apartments.yaml"
DOMRF_STOREHOUSES_PATH = CONFIGS_DIR / "domrf.yaml"


# ═══════════════════════════════════════════════════════
#  ЧТЕНИЕ
# ═══════════════════════════════════════════════════════

def _load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _save_yaml(path: Path, data: dict) -> None:
    """Сохранить YAML, сохраняя оригинальное форматирование комментариев.

    Перезаписывает файл полностью, но использует yaml.dump с настройками,
    максимально приближенными к оригиналу.
    """
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(
            data, f,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
            width=120,
        )


def list_links(include_storehouses: bool = True) -> list[dict]:
    """Получить список всех ссылок ДОМ.РФ.

    Возвращает список словарей с полями:
        object_id, complex_name, building, developer, city,
        in_apartments (bool), in_storehouses (bool)
    """
    apt_cfg = _load_yaml(DOMRF_APARTMENTS_PATH)
    apt_links = {l["object_id"]: l for l in apt_cfg.get("links", []) if l.get("object_id")}

    store_ids = set()
    if include_storehouses:
        store_cfg = _load_yaml(DOMRF_STOREHOUSES_PATH)
        store_ids = {l["object_id"] for l in store_cfg.get("links", []) if l.get("object_id")}

    result = []
    seen = set()
    for oid, link in apt_links.items():
        result.append({
            "object_id": oid,
            "complex_name": link.get("complex_name", ""),
            "building": link.get("building", ""),
            "developer": link.get("developer", ""),
            "city": link.get("city", "Казань"),
            "in_apartments": True,
            "in_storehouses": oid in store_ids,
        })
        seen.add(oid)

    # Ссылки, которые есть только в кладовках
    if include_storehouses:
        store_cfg = _load_yaml(DOMRF_STOREHOUSES_PATH)
        for link in store_cfg.get("links", []):
            oid = link.get("object_id")
            if oid and oid not in seen:
                result.append({
                    "object_id": oid,
                    "complex_name": link.get("complex_name", ""),
                    "building": "",
                    "developer": link.get("developer", ""),
                    "city": link.get("city", "Казань"),
                    "in_apartments": False,
                    "in_storehouses": True,
                })

    return sorted(result, key=lambda x: (x["complex_name"], x["building"], x["object_id"]))


def list_complexes() -> list[dict]:
    """Получить список уникальных ЖК с количеством объектов.

    Возвращает: [{complex_name, developer, city, object_count, buildings: [...]}]
    """
    links = list_links()
    complexes: dict[str, dict] = {}
    for link in links:
        cn = link["complex_name"]
        if cn not in complexes:
            complexes[cn] = {
                "complex_name": cn,
                "developer": link["developer"],
                "city": link["city"],
                "object_count": 0,
                "buildings": [],
            }
        complexes[cn]["object_count"] += 1
        if link["building"] and link["building"] not in complexes[cn]["buildings"]:
            complexes[cn]["buildings"].append(link["building"])

    return sorted(complexes.values(), key=lambda x: x["complex_name"])


# ═══════════════════════════════════════════════════════
#  ДОБАВЛЕНИЕ
# ═══════════════════════════════════════════════════════

def add_link(
    object_id: int,
    complex_name: str,
    building: str = "",
    developer: str = "",
    city: str = "Казань",
    add_to_apartments: bool = True,
    add_to_storehouses: bool = True,
) -> dict[str, bool]:
    """Добавить новую ссылку в конфиги ДОМ.РФ.

    Args:
        object_id: ID объекта на ДОМ.РФ
        complex_name: Название ЖК
        building: Корпус (обязателен для квартир, рекомендуется)
        developer: Застройщик
        city: Город (по умолчанию Казань)
        add_to_apartments: Добавить в конфиг квартир
        add_to_storehouses: Добавить в конфиг кладовок

    Returns:
        {"apartments": True/False, "storehouses": True/False}

    Raises:
        ValueError: если object_id или complex_name невалидны
    """
    # Валидация
    if not object_id or not isinstance(object_id, int) or object_id <= 0:
        raise ValueError(f"Невалидный object_id: {object_id}")
    if not complex_name or not complex_name.strip():
        raise ValueError("complex_name не может быть пустым")

    result = {"apartments": False, "storehouses": False}

    if add_to_apartments:
        if not building:
            logger.warning(
                "object_id=%d (%s): building не указан — "
                "квартиры будут привязаны к API-формату корпуса. "
                "Рекомендуется указать building для per-building данных.",
                object_id, complex_name,
            )
        result["apartments"] = _add_to_config(
            DOMRF_APARTMENTS_PATH, object_id, complex_name,
            building=building, developer=developer, city=city,
        )

    if add_to_storehouses:
        result["storehouses"] = _add_to_config(
            DOMRF_STOREHOUSES_PATH, object_id, complex_name,
            building="",  # кладовки не используют building
            developer=developer, city=city,
        )

    return result


def _add_to_config(
    config_path: Path,
    object_id: int,
    complex_name: str,
    building: str = "",
    developer: str = "",
    city: str = "Казань",
) -> bool:
    """Добавить ссылку в конкретный YAML-файл. Возвращает True если добавлено."""
    cfg = _load_yaml(config_path)
    links = cfg.get("links", [])

    # Проверить дубликат
    for existing in links:
        if existing.get("object_id") == object_id:
            existing_bld = existing.get("building", "")
            if existing_bld == building:
                logger.info(
                    "object_id=%d уже существует в %s (building=%r), пропускаем",
                    object_id, config_path.name, building,
                )
                return False

    # Дописываем запись в конец файла (сохраняя форматирование и комментарии)
    lines = [f'\n  - object_id: {object_id}']
    if building:
        lines.append(f'    building: "{building}"')
    lines.append(f'    complex_name: "{complex_name}"')
    if developer:
        lines.append(f'    developer: "{developer}"')
    lines.append(f'    city: "{city}"')
    lines.append('')  # пустая строка после записи

    with open(config_path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger.info(
        "Добавлен object_id=%d (%s / %s) в %s",
        object_id, complex_name, building or "-", config_path.name,
    )
    return True


# ═══════════════════════════════════════════════════════
#  УДАЛЕНИЕ
# ═══════════════════════════════════════════════════════

def remove_link(
    object_id: int,
    remove_from_apartments: bool = True,
    remove_from_storehouses: bool = True,
) -> dict[str, int]:
    """Удалить ссылку из конфигов.

    Returns:
        {"apartments": кол-во удалённых, "storehouses": кол-во удалённых}
    """
    result = {"apartments": 0, "storehouses": 0}

    if remove_from_apartments:
        result["apartments"] = _remove_from_config(DOMRF_APARTMENTS_PATH, object_id)

    if remove_from_storehouses:
        result["storehouses"] = _remove_from_config(DOMRF_STOREHOUSES_PATH, object_id)

    return result


def _remove_from_config(config_path: Path, object_id: int) -> int:
    """Удалить все записи с данным object_id из файла. Возвращает кол-во удалённых."""
    cfg = _load_yaml(config_path)
    links = cfg.get("links", [])
    original_count = len(links)

    cfg["links"] = [l for l in links if l.get("object_id") != object_id]
    removed = original_count - len(cfg["links"])

    if removed > 0:
        _save_yaml(config_path, cfg)
        logger.info(
            "Удалено %d записей object_id=%d из %s",
            removed, object_id, config_path.name,
        )
    return removed


# ═══════════════════════════════════════════════════════
#  МАССОВЫЕ ОПЕРАЦИИ
# ═══════════════════════════════════════════════════════

def add_links_batch(links: list[dict]) -> list[dict]:
    """Добавить несколько ссылок за раз.

    Args:
        links: список словарей с полями object_id, complex_name, building, developer, city

    Returns:
        список результатов [{object_id, apartments: bool, storehouses: bool}]
    """
    results = []
    for link in links:
        try:
            r = add_link(
                object_id=link["object_id"],
                complex_name=link["complex_name"],
                building=link.get("building", ""),
                developer=link.get("developer", ""),
                city=link.get("city", "Казань"),
                add_to_apartments=link.get("add_to_apartments", True),
                add_to_storehouses=link.get("add_to_storehouses", True),
            )
            results.append({"object_id": link["object_id"], **r})
        except ValueError as e:
            results.append({"object_id": link.get("object_id"), "error": str(e)})

    return results


def sync_configs() -> dict[str, int]:
    """Синхронизировать конфиги: object_id из apartments → storehouses и наоборот.

    Возвращает: {"added_to_apartments": N, "added_to_storehouses": N}
    """
    apt_cfg = _load_yaml(DOMRF_APARTMENTS_PATH)
    store_cfg = _load_yaml(DOMRF_STOREHOUSES_PATH)

    apt_ids = {l["object_id"]: l for l in apt_cfg.get("links", []) if l.get("object_id")}
    store_ids = {l["object_id"]: l for l in store_cfg.get("links", []) if l.get("object_id")}

    added_to_store = 0
    added_to_apt = 0

    # apt → store
    for oid, link in apt_ids.items():
        if oid not in store_ids:
            entry = {
                "object_id": oid,
                "complex_name": link.get("complex_name", ""),
            }
            if link.get("developer"):
                entry["developer"] = link["developer"]
            if link.get("city"):
                entry["city"] = link["city"]
            store_cfg["links"].append(entry)
            added_to_store += 1

    # store → apt
    for oid, link in store_ids.items():
        if oid not in apt_ids:
            entry = {
                "object_id": oid,
                "complex_name": link.get("complex_name", ""),
                "building": "",  # нужно заполнить вручную
            }
            if link.get("developer"):
                entry["developer"] = link["developer"]
            if link.get("city"):
                entry["city"] = link["city"]
            apt_cfg["links"].append(entry)
            added_to_apt += 1
            logger.warning(
                "object_id=%d добавлен в apartments с пустым building — "
                "заполните building вручную!",
                oid,
            )

    if added_to_store:
        _save_yaml(DOMRF_STOREHOUSES_PATH, store_cfg)
    if added_to_apt:
        _save_yaml(DOMRF_APARTMENTS_PATH, apt_cfg)

    logger.info(
        "Синхронизация: +%d в storehouses, +%d в apartments",
        added_to_store, added_to_apt,
    )
    return {"added_to_apartments": added_to_apt, "added_to_storehouses": added_to_store}
