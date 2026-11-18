from lakehouse.config import expand_env, load_config


def test_env_expansion(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_PATH", "s3a://prod-bucket")
    monkeypatch.delenv("RAW_PATH", raising=False)
    assert expand_env("${LAKEHOUSE_PATH:-s3a://lakehouse}/silver") == "s3a://prod-bucket/silver"
    assert expand_env("${RAW_PATH:-hdfs://nn:9000/raw}") == "hdfs://nn:9000/raw"


def test_load_default_config(monkeypatch):
    monkeypatch.delenv("LAKEHOUSE_PATH", raising=False)
    cfg = load_config()
    assert cfg.paths.gold == "s3a://lakehouse/gold"
    assert cfg.skew.salt_buckets > 1
    assert cfg.quality_for("silver")["max_reject_ratio"] > 0
