#!/usr/bin/env python3
"""Запуск парсера квартир УниСтрой + экспорт в xlsx."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from parsers.unistroy_apartments import UnistroyApartmentParser
from runners.runner_utils import run_apartment_parser


async def main():
    parser = UnistroyApartmentParser()
    result = await run_apartment_parser(parser, "unistroy", "УниСтрой")
    return result.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
