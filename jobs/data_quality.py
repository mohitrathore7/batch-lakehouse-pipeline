"""Data-quality gate: run the check suite for one layer and one ds."""

from __future__ import annotations

from lakehouse.cli import base_parser, setup_logging
from lakehouse.config import load_config
from lakehouse.quality import SUITES, run_suite
from lakehouse.session import build_spark


def main() -> None:
    setup_logging()
    parser = base_parser(__doc__)
    parser.add_argument("--layer", required=True, choices=sorted(SUITES))
    args = parser.parse_args()

    cfg = load_config(args.config)
    spark = build_spark(f"dq_{args.layer}_{args.ds}", cfg, master=args.master)
    try:
        run_suite(spark, cfg, args.layer, args.ds)  # raises DataQualityError -> task fails
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
