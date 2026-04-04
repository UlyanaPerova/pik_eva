#!/usr/bin/env python3
"""Запуск парсера СМУ-88 (кладовки) + экспорт в xlsx."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from parsers.smu88 import Smu88Parser
from runners.runner_utils import run_storehouse_parser


async def main():
    parser = Smu88Parser()
    result = await run_storehouse_parser(
        parser, "smu88", "СМУ-88",
        export_filename="storehouses_SMU88.xlsx",
    )
    return result.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
