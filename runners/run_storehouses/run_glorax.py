#!/usr/bin/env python3
"""Запуск парсера GloraX (кладовки) + экспорт в xlsx."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from parsers.glorax import GloraxParser
from runners.runner_utils import run_storehouse_parser


async def main():
    parser = GloraxParser()
    result = await run_storehouse_parser(
        parser, "glorax", "GloraX",
        export_filename="storehouses_GloraX.xlsx",
    )
    return result.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
