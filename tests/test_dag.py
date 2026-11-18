"""DAG integrity checks. Skipped when Airflow is not installed (e.g. in CI)."""

import os
from pathlib import Path

import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.apache.spark")

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def dag():
    os.environ["PROJECT_DIR"] = str(ROOT)
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.get_dag("batch_lakehouse_pipeline")


def test_task_order(dag):
    chain = ["start", "bronze_ingest", "dq_bronze", "silver_transform", "dq_silver",
             "gold_aggregations", "dq_gold", "end"]
    for upstream, downstream in zip(chain, chain[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids


def test_retries_and_backfill_settings(dag):
    assert dag.catchup is True
    for task in dag.tasks:
        assert task.retries == 3
        assert task.retry_exponential_backoff is True
