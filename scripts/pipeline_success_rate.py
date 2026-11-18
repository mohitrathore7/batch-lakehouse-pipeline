"""Report the DAG-run success rate from the Airflow REST API.

  python scripts/pipeline_success_rate.py --url http://localhost:8081 --user admin --password admin
"""

from __future__ import annotations

import argparse
import base64
import json
import urllib.request
from collections import Counter


def fetch_runs(url: str, dag_id: str, auth: str) -> list[dict]:
    runs, offset = [], 0
    while True:
        req = urllib.request.Request(
            f"{url}/api/v1/dags/{dag_id}/dagRuns?limit=100&offset={offset}",
            headers={"Authorization": f"Basic {auth}"},
        )
        with urllib.request.urlopen(req) as resp:
            page = json.load(resp)
        runs.extend(page["dag_runs"])
        offset += len(page["dag_runs"])
        if offset >= page["total_entries"] or not page["dag_runs"]:
            return runs


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:8081")
    p.add_argument("--dag-id", default="batch_lakehouse_pipeline")
    p.add_argument("--user", default="admin")
    p.add_argument("--password", default="admin")
    args = p.parse_args()

    auth = base64.b64encode(f"{args.user}:{args.password}".encode()).decode()
    states = Counter(r["state"] for r in fetch_runs(args.url, args.dag_id, auth))
    finished = states["success"] + states["failed"]
    rate = states["success"] / finished * 100 if finished else 0.0

    print(f"DAG runs: {dict(states)}")
    print(f"Success rate (finished runs): {rate:.1f}% ({states['success']}/{finished})")


if __name__ == "__main__":
    main()
