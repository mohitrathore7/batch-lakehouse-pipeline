"""Shared command-line handling for the Spark job entrypoints."""

from __future__ import annotations

import argparse
import logging
from datetime import date


def _valid_ds(value: str) -> str:
    date.fromisoformat(value)  # raises on a malformed date
    return value


def base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--ds", required=True, type=_valid_ds,
                        help="Logical run date (YYYY-MM-DD); Airflow passes {{ ds }}")
    parser.add_argument("--config", default=None, help="Path to pipeline.yaml")
    parser.add_argument("--mode", choices=["optimized", "baseline"], default="optimized")
    parser.add_argument("--master", default=None,
                        help="Spark master; omit when launched through spark-submit")
    return parser


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s - %(message)s")
