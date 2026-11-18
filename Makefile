SPARK_SUBMIT = docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077
RAW = hdfs://namenode:9000/data/raw

.PHONY: help install test package jar up down generate run backfill benchmark local clean

help:
	@echo "install    - install Python dev dependencies"
	@echo "test       - run unit tests (PySpark local mode)"
	@echo "package    - build dist/lakehouse.zip (shipped to Spark with --py-files)"
	@echo "jar        - build the Scala gold-layer jar (needs Docker)"
	@echo "up / down  - start / stop the HDFS + Spark + MinIO + Airflow stack"
	@echo "generate   - write 5M synthetic raw orders to HDFS"
	@echo "run        - unpause the DAG so Airflow runs every day in the window"
	@echo "backfill   - rerun a date range, e.g. make backfill START=2024-01-01 END=2024-01-03"
	@echo "benchmark  - baseline vs optimized runtime on local Spark"
	@echo "local      - run bronze -> silver -> DQ locally on a small dataset"

install:
	pip install -r requirements-dev.txt

test:
	PYTHONPATH=src pytest -q

package:
	mkdir -p dist
	cd src && zip -qr ../dist/lakehouse.zip lakehouse -x '*__pycache__*'

jar:
	docker run --rm -v "$(PWD)/scala":/app -w /app sbtscala/scala-sbt:eclipse-temurin-17.0.21_9_1.13.1_2.12.21 sbt test assembly

up: package
	docker compose up -d --build

down:
	docker compose down

generate:
	$(SPARK_SUBMIT) /opt/project/data_generator/generate_data.py --output $(RAW) --orders 5000000 --days 10

run:
	docker compose exec airflow-scheduler airflow dags unpause batch_lakehouse_pipeline

backfill:
	docker compose exec airflow-scheduler airflow dags backfill batch_lakehouse_pipeline \
		-s $(START) -e $(END) --reset-dagruns -y

benchmark:
	PYTHONPATH=src python benchmarks/benchmark_runtime.py --orders 5000000 --repeats 3

local:
	PYTHONPATH=src python scripts/run_local.py --orders 200000

clean:
	rm -rf dist data spark-warehouse .pytest_cache
