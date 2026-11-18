"""Daily batch lakehouse pipeline.

raw CSV (HDFS) -> bronze -> DQ -> silver -> DQ -> gold (Scala) -> DQ

* One DAG run processes exactly one logical date ({{ ds }}).
* Every write uses dynamic partition overwrite, so retries, reruns and
  backfills (`airflow dags backfill`) are idempotent.
* Tasks retry automatically with exponential backoff.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/project")
CONFIG_PATH = f"{PROJECT_DIR}/conf/pipeline.yaml"
PY_FILES = f"{PROJECT_DIR}/dist/lakehouse.zip"
GOLD_JAR = f"{PROJECT_DIR}/scala/target/scala-2.12/lakehouse-gold.jar"
SPARK_CONN_ID = "spark_default"

# hadoop-aws must match the Hadoop version bundled with Spark 3.5 (3.3.4).
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

SPARK_CONF = {
    "spark.driver.host": os.environ.get("SPARK_DRIVER_HOST", "airflow-scheduler"),
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.hadoop.fs.s3a.endpoint": os.environ.get("S3_ENDPOINT", "s3.amazonaws.com"),
    "spark.hadoop.fs.s3a.path.style.access": "true" if os.environ.get("S3_ENDPOINT") else "false",
}

log = logging.getLogger(__name__)


def _lakehouse_paths() -> dict:
    """Resolve paths from the same YAML the Spark jobs use (needed by the Scala job)."""
    import sys

    sys.path.insert(0, f"{PROJECT_DIR}/src")
    from lakehouse.config import load_config  # only needs PyYAML, not Spark

    paths = load_config(CONFIG_PATH).paths
    return {"silver": paths.silver, "gold": paths.gold}


PATHS = _lakehouse_paths()


def notify_failure(context) -> None:
    ti = context["task_instance"]
    log.error("Task %s failed for ds=%s after %s tries. Log: %s",
              ti.task_id, context["ds"], ti.try_number, ti.log_url)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": notify_failure,
}


def pyspark_task(task_id: str, script: str, *extra_args: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=f"{PROJECT_DIR}/jobs/{script}",
        py_files=PY_FILES,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        name=f"{task_id}_{{{{ ds_nodash }}}}",
        application_args=["--ds", "{{ ds }}", "--config", CONFIG_PATH, *extra_args],
        executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
    )


with DAG(
    dag_id="batch_lakehouse_pipeline",
    description="HDFS raw -> S3 Parquet lakehouse (bronze/silver/gold) with DQ gates",
    start_date=datetime.fromisoformat(os.environ.get("PIPELINE_START_DATE", "2024-01-01")),
    end_date=datetime.fromisoformat(os.environ.get("PIPELINE_END_DATE", "2024-01-10")),
    schedule="@daily",
    catchup=True,
    max_active_runs=2,
    default_args=default_args,
    tags=["spark", "lakehouse", "batch"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")

    bronze = pyspark_task("bronze_ingest", "bronze_ingest.py")
    dq_bronze = pyspark_task("dq_bronze", "data_quality.py", "--layer", "bronze")

    silver = pyspark_task("silver_transform", "silver_transform.py")
    dq_silver = pyspark_task("dq_silver", "data_quality.py", "--layer", "silver")

    gold = SparkSubmitOperator(
        task_id="gold_aggregations",
        conn_id=SPARK_CONN_ID,
        application=GOLD_JAR,
        java_class="com.mohitrathore.lakehouse.GoldAggregations",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        name="gold_aggregations_{{ ds_nodash }}",
        application_args=["--ds", "{{ ds }}",
                          "--silver-path", PATHS["silver"],
                          "--gold-path", PATHS["gold"]],
        executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
    )
    dq_gold = pyspark_task("dq_gold", "data_quality.py", "--layer", "gold")

    end = EmptyOperator(task_id="end")

    start >> bronze >> dq_bronze >> silver >> dq_silver >> gold >> dq_gold >> end
