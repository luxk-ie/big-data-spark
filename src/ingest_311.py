"""Download an unchanged NYC 311 CSV snapshot with provenance and failure evidence."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import uuid

ROOT = Path(__file__).resolve().parents[1]


def ingest(url, raw_root, dataset="nyc_311", filename="source.csv"):
    if Path(filename).name != filename or filename in {"", ".", ".."}:
        raise ValueError("filename must be a basename")
    if Path(dataset).name != dataset or dataset in {"", ".", ".."}:
        raise ValueError("dataset must be a single directory name")
    now = datetime.now(timezone.utc)
    run = raw_root / dataset / f"ingest_date={now:%Y-%m-%d}" / f"run_{now:%Y%m%dT%H%M%S%fZ}_{uuid.uuid4().hex[:8]}"
    run.mkdir(parents=True, exist_ok=False)
    destination = run / filename
    partial = run / (filename + ".part")
    manifest = dict(dataset=dataset, source=url, ingested_at_utc=now.isoformat(),
                    bytes=0, sha256=None, destination=str(destination.resolve()), status="failed")
    digest = hashlib.sha256()
    try:
        request = Request(url, headers={"User-Agent": "BigDataCourseCheckpoint/1.0", "Accept": "text/csv"})
        with urlopen(request, timeout=120) as response, partial.open("wb") as output:
            content_type = response.headers.get("Content-Type", "")
            if "html" in content_type.lower() or "json" in content_type.lower():
                raise ValueError(f"Unexpected Content-Type: {content_type}")
            expected_size = response.headers.get("Content-Length")
            manifest["resolved_source"] = response.url
            manifest["content_type"] = content_type
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                digest.update(chunk)
                manifest["bytes"] += len(chunk)
        manifest["sha256"] = digest.hexdigest()
        if expected_size is not None and manifest["bytes"] != int(expected_size):
            raise ValueError("Incomplete HTTP response")
        if manifest["bytes"] == 0:
            raise ValueError("Empty download")
        if filename.endswith(".parquet"):
            with partial.open("rb") as stream:
                if stream.read(4) != b"PAR1":
                    raise ValueError("Invalid Parquet header")
                stream.seek(-4, 2)
                if stream.read(4) != b"PAR1":
                    raise ValueError("Invalid Parquet footer")
            # Decode metadata too; magic bytes alone do not prove readable Parquet.
            import pyarrow.parquet as pq
            with pq.ParquetFile(partial) as parquet:
                if parquet.metadata.num_rows <= 0:
                    raise ValueError("Parquet contains no rows")
                manifest["rows"] = parquet.metadata.num_rows
        else:
            with partial.open(encoding="utf-8-sig", newline="") as stream:
                reader = csv.reader(stream)
                header = next(reader, [])
                sample = next(reader, [])
                normalized = {name.strip().lower().replace(" ", "_") for name in header}
                if not {"unique_key", "created_date"}.issubset(normalized) or len(sample) != len(header):
                    raise ValueError("Invalid CSV schema or no data row")
                rows = 1 + sum(1 for _ in reader)
                if rows >= 1000000:
                    raise ValueError("CSV reached API row cap; narrow the date interval")
                manifest["rows"] = rows
        partial.rename(destination)
        manifest["status"] = "success"
    except Exception as error:
        manifest["error"] = f"{type(error).__name__}: {error}"
        if partial.exists():
            manifest["bytes"] = partial.stat().st_size
            with partial.open("rb") as stream:
                manifest["sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
        raise
    finally:
        (run / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        log = ROOT / "logs" / "ingest.jsonl"
        log.parent.mkdir(exist_ok=True)
        with log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(manifest, ensure_ascii=False) + "\n")
        print(json.dumps(manifest, ensure_ascii=False), flush=True)
    return destination


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2025-02-01")
    parser.add_argument("--raw-root", type=Path, default=ROOT / "data/raw")
    parser.add_argument("--url", help="Override URL, including a local HTTP fixture for tests")
    args = parser.parse_args()
    start, end = datetime.fromisoformat(args.start), datetime.fromisoformat(args.end)
    if start >= end:
        parser.error("start must precede end")
    query = urlencode({"$where": f"created_date >= '{start:%Y-%m-%dT00:00:00}' AND created_date < '{end:%Y-%m-%dT00:00:00}'",
                       "$order": "created_date,unique_key", "$limit": 1000000})
    url = args.url or "https://data.cityofnewyork.us/resource/erm2-nwe9.csv?" + query
    ingest(url, args.raw_root)


if __name__ == "__main__":
    main()
