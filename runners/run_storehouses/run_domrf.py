#!/usr/bin/env python3
"""Запуск парсера ДОМ.РФ (кладовки) + экспорт в xlsx."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from parsers.domrf import DomRfParser
from runners.runner_utils import run_storehouse_parser


def _parse_args():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--cdp", type=int, default=None, metavar="PORT",
                    help="Подключиться к Chrome через CDP (порт remote-debugging)")
    return ap.parse_args()


async def main():
    args = _parse_args()
    parser = DomRfParser(cdp_port=args.cdp)
    result = await run_storehouse_parser(
        parser, "domrf", "ДОМ.РФ",
        export_filename="storehouses_DomRF.xlsx",
    )
    return result.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
