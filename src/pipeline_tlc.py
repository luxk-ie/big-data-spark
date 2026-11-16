"""Checkpoint 3: reproducible pickup demand pipeline for one TLC raw snapshot."""
import argparse
import csv
import hashlib
import io
import json
import os
import platform
import time
import urllib.request
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "data/raw/nyc_tlc_yellow/ingest_date=2026-10-23/run_20261023T085112919770Z_3632e145/manifest.json"
DEFAULT_ZONES = ROOT / "data/reference/taxi_zone_lookup.csv"
SOURCE_NAME = "yellow_tripdata_2025-01.parquet"


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_manifest(path):
    path = Path(path).resolve()
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("status") != "success" or manifest.get("dataset") != "nyc_tlc_yellow":
        raise ValueError("Manifest must be a successful nyc_tlc_yellow run")
    destination = Path(manifest.get("destination", ""))
    if destination.name != SOURCE_NAME:
        raise ValueError("Unexpected raw filename in manifest")
    sibling = path.parent / SOURCE_NAME
    raw = destination if destination.is_file() else sibling
    if not raw.is_file():
        raise FileNotFoundError(f"Raw file missing: {destination} or {sibling}")
    if raw.stat().st_size != manifest.get("bytes"):
        raise ValueError("Raw byte size differs from manifest")
    if sha256(raw) != manifest.get("sha256"):
        raise ValueError("Raw SHA-256 differs from manifest")
    return manifest, raw


def validate_zones(path):
    path = Path(path).resolve()
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError("Taxi Zone Lookup CSV missing or empty")
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not {"LocationID", "Borough", "Zone", "service_zone"}.issubset(reader.fieldnames or []):
            raise ValueError("Taxi Zone Lookup columns missing")
        rows = list(reader)
    if not rows:
        raise ValueError("Taxi Zone Lookup has no data")
    keys = [row["LocationID"] for row in rows]
    if any(not key or not key.isdecimal() for key in keys) or len({int(key) for key in keys}) != len(keys):
        raise ValueError("Taxi Zone Lookup LocationID must be non-null and unique")
    return path, len(rows), sha256(path)


def transformed(raw):
    required = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "trip_distance", "total_amount"}
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(f"Raw columns missing: {sorted(missing)}")
    # Preserve source timestamp_ntz wall-clock values. No UTC conversion is inferred.
    return (raw.select(
        F.col("tpep_pickup_datetime").cast("timestamp_ntz").alias("pickup"),
        F.col("tpep_dropoff_datetime").cast("timestamp_ntz").alias("dropoff"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("total_amount").cast("double").alias("total_amount"),
    ).withColumn("in_scope", (F.col("pickup") >= F.lit("2025-01-01 00:00:00").cast("timestamp_ntz")) &
                 (F.col("pickup") < F.lit("2025-02-01 00:00:00").cast("timestamp_ntz")))
     .withColumn("invalid_dropoff", F.col("dropoff").isNull() | (F.col("dropoff") < F.col("pickup")))
     .withColumn("invalid_distance", F.col("trip_distance").isNull() | (F.col("trip_distance") < 0))
     .withColumn("invalid_amount", F.col("total_amount").isNull() | (F.col("total_amount") < 0)))


def demand(facts, zones):
    if zones.filter(F.col("LocationID").isNull()).limit(1).count():
        raise ValueError("Null LocationID in lookup")
    if zones.groupBy("LocationID").count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate LocationID in lookup")
    scoped = facts.filter(F.col("in_scope"))
    joined = scoped.join(zones, scoped.pickup_location_id == zones.LocationID, "left")
    joined = joined.withColumn("zone_status", F.when(F.col("pickup_location_id").isNull(), "null_key")
                               .when(F.col("LocationID").isNull(), "unmatched").otherwise("matched"))
    joined = joined.withColumn("pickup_borough", F.when(F.col("zone_status") == "null_key", "Unknown (null key)")
                               .when(F.col("zone_status") == "unmatched", "Unknown (unmatched key)")
                               .otherwise(F.coalesce(F.col("Borough"), F.lit("Unknown (lookup null)"))))
    gold = (joined.withColumn("pickup_date", F.to_date("pickup"))
            .withColumn("pickup_hour", F.hour("pickup"))
            .groupBy("pickup_date", "pickup_hour", "pickup_borough")
            .agg(F.count(F.lit(1)).alias("trip_count")))
    return joined, gold


def counts(facts, joined, gold, raw_count):
    flags = facts.agg(
        F.sum(F.col("pickup").isNull().cast("long")).alias("invalid_pickup_timestamp_rows"),
        F.sum((F.col("pickup").isNotNull() & ~F.col("in_scope")).cast("long")).alias("out_of_scope_rows"),
        F.sum(F.col("in_scope").cast("long")).alias("in_scope_rows"),
        F.sum((F.col("in_scope") & F.col("invalid_dropoff")).cast("long")).alias("invalid_dropoff_rows"),
        F.sum((F.col("in_scope") & F.col("invalid_distance")).cast("long")).alias("invalid_distance_rows"),
        F.sum((F.col("in_scope") & F.col("invalid_amount")).cast("long")).alias("invalid_amount_rows"),
    ).first().asDict()
    flags = {key: int(value or 0) for key, value in flags.items()}
    flags["raw_input_rows"] = raw_count
    flags["valid_rows"] = flags["in_scope_rows"]
    flags["rejected_rows"] = 0  # Optional measure errors do not remove pickup demand.
    if raw_count != flags["invalid_pickup_timestamp_rows"] + flags["out_of_scope_rows"] + flags["in_scope_rows"]:
        raise AssertionError("Raw accounting failed")
    statuses = {row["zone_status"]: row["count"] for row in joined.groupBy("zone_status").count().collect()}
    flags["zone_status"] = statuses
    flags["joined_rows"] = sum(statuses.values())
    if flags["joined_rows"] != flags["in_scope_rows"]:
        raise AssertionError("Join multiplied or removed facts")
    totals = gold.agg(F.count("*").alias("output_rows"), F.sum("trip_count").alias("trip_count")).first()
    flags["output_rows"] = totals["output_rows"]
    flags["trip_count"] = int(totals["trip_count"] or 0)
    if flags["trip_count"] != flags["in_scope_rows"]:
        raise AssertionError("Gold total differs from in-scope facts")
    if gold.groupBy("pickup_date", "pickup_hour", "pickup_borough").count().filter("count > 1").limit(1).count():
        raise AssertionError("Gold grain is not unique")
    return flags


def capture_stages(spark, output):
    ui = spark.sparkContext.uiWebUrl
    if not ui:
        raise RuntimeError("Spark UI is required for Checkpoint 4 stage evidence")
    url = f"{ui}/api/v1/applications/{spark.sparkContext.applicationId}/stages?status=complete"
    with urllib.request.urlopen(url, timeout=15) as response:
        stages = json.load(response)
    if not stages:
        raise RuntimeError("Spark UI returned no completed stages")
    fields = ("stageId", "attemptId", "name", "submissionTime", "completionTime", "numTasks",
              "executorRunTime", "jvmGcTime", "inputBytes", "inputRecords", "shuffleReadBytes",
              "shuffleWriteBytes", "memoryBytesSpilled", "diskBytesSpilled")
    summary = [{key: stage.get(key) for key in fields} for stage in stages]
    (output / "stages.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    jobs_url = f"{ui}/api/v1/applications/{spark.sparkContext.applicationId}/jobs?status=succeeded"
    with urllib.request.urlopen(jobs_url, timeout=15) as response:
        jobs = json.load(response)
    job_fields = ("jobId", "name", "stageIds", "submissionTime", "completionTime", "numTasks")
    (output / "jobs.json").write_text(json.dumps(
        [{key: job.get(key) for key in job_fields} for job in jobs], indent=2), encoding="utf-8")
    return len(summary)


def run(manifest_path, zones_path, output_root, stage_evidence=False):
    started = time.perf_counter()
    manifest, raw_path = validate_manifest(manifest_path)
    zones_path, zone_count, zone_hash = validate_zones(zones_path)
    code_hash = sha256(__file__)
    version = f"raw_{manifest['sha256'][:12]}_zone_{zone_hash[:12]}_code_{code_hash[:12]}"
    output = Path(output_root).resolve() / version
    output.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.master("local[*]").appName("TLC Checkpoint 3").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")  # No conversion of timestamp_ntz values.
    try:
        raw = spark.read.parquet(str(raw_path)).cache()
        facts = transformed(raw)
        zone_frame = (spark.read.option("header", True).option("inferSchema", False).csv(str(zones_path))
                      .select(F.col("LocationID").cast("int").alias("LocationID"), "Borough"))
        joined, gold = demand(facts, zone_frame)
        raw_count = raw.count()
        action_seconds = {}
        action_started = time.perf_counter()
        metrics = counts(facts, joined, gold, raw_count)
        action_seconds["quality_and_gold_checks"] = round(time.perf_counter() - action_started, 3)
        # Whole-row duplicates are observed, never removed. Use raw columns, not reduced projection.
        action_started = time.perf_counter()
        metrics["duplicate_full_rows"] = raw_count - raw.distinct().count()
        action_seconds["duplicate_full_rows"] = round(time.perf_counter() - action_started, 3)
        plan_stream = io.StringIO()
        with redirect_stdout(plan_stream):
            gold.explain("formatted")
        plan = plan_stream.getvalue()
        (output / "plan.txt").write_text(plan, encoding="utf-8")
        # Spark's Windows Hadoop writer requires winutils.exe, absent from this local setup.
        # Gold is at most 31 * 24 * zone-groups rows; collect only this aggregate.
        action_started = time.perf_counter()
        gold_rows = sorted(gold.collect(), key=lambda row: (row.pickup_date, row.pickup_hour, row.pickup_borough))
        action_seconds["gold_collect"] = round(time.perf_counter() - action_started, 3)
        table = pa.table({
            "pickup_date": pa.array([row.pickup_date for row in gold_rows], type=pa.date32()),
            "pickup_hour": pa.array([row.pickup_hour for row in gold_rows], type=pa.int32()),
            "pickup_borough": pa.array([row.pickup_borough for row in gold_rows], type=pa.string()),
            "trip_count": pa.array([row.trip_count for row in gold_rows], type=pa.int64()),
        })
        pq.write_table(table, output / "gold.parquet")
        read_back = spark.read.parquet(str(output / "gold.parquet"))
        if read_back.count() != metrics["output_rows"]:
            raise AssertionError("Gold read-back row count differs")
        preview = [row.asDict(recursive=True) for row in gold_rows[:20]]
        with (output / "preview.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=["pickup_date", "pickup_hour", "pickup_borough", "trip_count"])
            writer.writeheader()
            writer.writerows(preview)
        elapsed = round(time.perf_counter() - started, 3)
        stage_count = capture_stages(spark, output) if stage_evidence else None
        evidence = {"status": "VERIFIED", "measured_at_utc": datetime.now(timezone.utc).isoformat(),
                    "manifest": str(Path(manifest_path).resolve()), "raw_sha256": manifest["sha256"],
                    "raw_bytes": raw_path.stat().st_size, "lookup": str(zones_path), "lookup_sha256": zone_hash,
                    "lookup_bytes": zones_path.stat().st_size, "lookup_rows": zone_count,
                    "code_sha256": code_hash, "version": version, "metrics": metrics,
                    "output_schema": read_back.schema.jsonValue(), "raw_partitions": raw.rdd.getNumPartitions(),
                    "gold_partitions": gold.rdd.getNumPartitions(), "spark_version": spark.version,
                    "python_version": platform.python_version(), "master": spark.sparkContext.master,
                    "elapsed_seconds": elapsed}
        if stage_evidence:
            evidence["benchmark_context"] = {
                "logical_cores": os.cpu_count(), "spark_default_parallelism": spark.sparkContext.defaultParallelism,
                "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory", "not explicitly set"),
                "aqe_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
                "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
                "completed_stages": stage_count,
            }
            evidence["action_seconds"] = action_seconds
        (output / "evidence.json").write_text(json.dumps(evidence, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        print(json.dumps({"output": str(output), "metrics": metrics, "elapsed_seconds": evidence["elapsed_seconds"]}, ensure_ascii=False))
        return evidence
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--zones", type=Path, default=DEFAULT_ZONES)
    parser.add_argument("--output", type=Path, default=ROOT / "data/processed/nyc_tlc_yellow")
    parser.add_argument("--stage-evidence", action="store_true", help="Save completed Spark stage metrics from the live UI")
    args = parser.parse_args()
    run(args.manifest, args.zones, args.output, args.stage_evidence)
