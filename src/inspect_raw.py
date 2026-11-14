"""Verify one manifest and inspect its CSV with local Spark without modifying raw."""
import argparse
import hashlib
import json
from pathlib import Path
from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    meta = json.loads(args.manifest.read_text(encoding="utf-8"))
    path = Path(meta["destination"])
    assert meta["status"] == "success", "Manifest failed"
    assert path.stat().st_size == meta["bytes"] > 0, "Size mismatch"
    with path.open("rb") as stream:
        assert hashlib.file_digest(stream, "sha256").hexdigest() == meta["sha256"], "Checksum mismatch"
    spark = SparkSession.builder.master("local[*]").appName("InspectNYC311Raw").getOrCreate()
    try:
        if path.suffix == ".parquet":
            frame = spark.read.parquet(str(path))
        else:
            frame = spark.read.option("header", True).option("multiLine", True).option("escape", '"').option("mode", "FAILFAST").csv(str(path))
        count = frame.count()
        assert count > 0, "No rows"
        frame.printSchema()
        frame.show(5, truncate=60)
        result = dict(status="PASS", master=spark.sparkContext.master, spark_version=spark.version,
                      manifest=str(args.manifest.resolve()), rows=count, columns=len(frame.columns), sha256=meta["sha256"])
        print(json.dumps(result))
        root = Path(__file__).resolve().parents[1]
        (root / "metadata/raw_inspection.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
