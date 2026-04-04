#!/usr/bin/env python3
"""Запуск парсера ПИК (кладовки) + экспорт в xlsx."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from parsers.pik import PikParser
from runners.runner_utils import run_storehouse_parser


async def main():
    parser = PikParser()
    result = await run_storehouse_parser(parser, "pik", "ПИК")
    return result.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
