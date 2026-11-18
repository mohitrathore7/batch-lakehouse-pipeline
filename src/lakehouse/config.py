"""Loads conf/pipeline.yaml into a typed config object."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "conf" / "pipeline.yaml"


def expand_env(value: Any) -> Any:
    """Recursively expand ${VAR} and ${VAR:-default} in strings."""
    if isinstance(value, str):
        def _sub(match: re.Match) -> str:
            name, default = match.group(1), match.group(2)
            resolved = os.environ.get(name, default)
            if resolved is None:
                raise KeyError(f"Environment variable {name} is not set and has no default")
            return resolved

        return _ENV_PATTERN.sub(_sub, value)
    if isinstance(value, dict):
        return {k: expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v) for v in value]
    return value


@dataclass(frozen=True)
class Paths:
    raw: str
    bronze: str
    silver: str
    gold: str
    quality: str


@dataclass(frozen=True)
class SparkSettings:
    shuffle_partitions: int = 64
    output_files_per_partition: int = 4
    broadcast_threshold_mb: int = 64


@dataclass(frozen=True)
class SkewSettings:
    hot_key_min_share: float = 0.005
    max_hot_keys: int = 50
    salt_buckets: int = 16


@dataclass(frozen=True)
class PipelineConfig:
    paths: Paths
    spark: SparkSettings = field(default_factory=SparkSettings)
    skew: SkewSettings = field(default_factory=SkewSettings)
    quality: dict = field(default_factory=dict)

    def quality_for(self, layer: str) -> dict:
        return dict(self.quality.get(layer, {}))


def load_config(path: str | os.PathLike | None = None) -> PipelineConfig:
    config_path = Path(path or os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG_PATH))
    with open(config_path, encoding="utf-8") as fh:
        raw = expand_env(yaml.safe_load(fh))

    return PipelineConfig(
        paths=Paths(**raw["paths"]),
        spark=SparkSettings(**raw.get("spark", {})),
        skew=SkewSettings(**raw.get("skew", {})),
        quality=raw.get("quality", {}),
    )
