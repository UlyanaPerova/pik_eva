#!/usr/bin/env python3
"""Запуск парсера Ак Бар Дом (кладовки) + экспорт в xlsx."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from parsers.akbarsdom import AkBarsDomParser
from runners.runner_utils import run_storehouse_parser


async def main():
    parser = AkBarsDomParser()
    result = await run_storehouse_parser(parser, "akbarsdom", "Ак Бар Дом")
    return result.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
