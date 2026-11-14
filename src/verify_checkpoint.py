"""Recompute checksums for successful raw runs and create a small derived sample."""
import hashlib
import json
from pathlib import Path
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]


def main():
    results = []
    for file in sorted((ROOT / "data/raw/nyc_tlc_yellow").rglob("manifest.json")):
        meta = json.loads(file.read_text(encoding="utf-8"))
        if meta["status"] != "success":
            continue
        path = Path(meta["destination"])
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        assert digest == meta["sha256"] and path.stat().st_size == meta["bytes"] > 0
        results.append({"manifest": str(file.relative_to(ROOT)), "bytes": meta["bytes"], "sha256": digest,
                        "rows": meta["rows"], "run_id": file.parent.name})
    assert len(results) >= 3, "Need three successful real ingestion runs"
    with pq.ParquetFile(Path(json.loads((ROOT / results[0]["manifest"]).read_text())["destination"])) as parquet:
        batch = next(parquet.iter_batches(batch_size=1000))
        import pyarrow as pa
        sample = ROOT / "data/sample/tlc_yellow_2025-01_1000.parquet"
        pq.write_table(pa.Table.from_batches([batch]), sample)
    report = {"status": "PASS", "successful_runs": len(results), "runs": results,
              "sample": str(sample.relative_to(ROOT)), "sample_rows": batch.num_rows,
              "sample_derived_from": results[0]["manifest"],
              "policy": "Separate snapshot per run; equal hashes identify duplicate content; select one run for analysis."}
    (ROOT / "metadata/rerun_verification.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
