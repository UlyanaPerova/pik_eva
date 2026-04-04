#!/usr/bin/env python3
"""Запуск парсера квартир СМУ-88 + экспорт в xlsx."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from parsers.smu88_apartments import Smu88ApartmentParser
from runners.runner_utils import run_apartment_parser


async def main():
    parser = Smu88ApartmentParser()
    result = await run_apartment_parser(parser, "smu88", "СМУ-88")
    return result.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
