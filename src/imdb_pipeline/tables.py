"""Load cleaned tables from one validated, consistently published snapshot."""

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from imdb_pipeline.config import CLEAN_DIR


def load_clean_tables(
    spark: SparkSession,
    names=("title.basics", "title.ratings"),
    clean_dir: Path = CLEAN_DIR,
) -> dict[str, DataFrame]:
    """Read the publication once so related tables use the same source run."""
    root = Path(clean_dir).resolve()
    manifest = json.loads((root / "current.json").read_text(encoding="utf-8"))
    if manifest.get("contract_version") != 1:
        raise ValueError("Unsupported cleaned-table contract version")
    frames = {}
    for name in names:
        path = (root / manifest["tables"][name]["path"]).resolve()
        if not path.is_relative_to(root):
            raise ValueError(f"Table path is outside the clean directory: {name}")
        frames[name] = spark.read.parquet(str(path))
    return frames
