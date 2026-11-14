"""Download an unchanged Parquet snapshot with provenance and failure evidence."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.request import Request, urlopen
import uuid


ROOT = Path(__file__).resolve().parents[1]


def ingest(url, raw_root, dataset, filename):
    """Land one immutable Parquet snapshot and write its manifest."""
    if Path(filename).name != filename or filename in {"", ".", ".."}:
        raise ValueError("filename must be a basename")
    if not filename.endswith(".parquet"):
        raise ValueError("filename must end with .parquet")
    if Path(dataset).name != dataset or dataset in {"", ".", ".."}:
        raise ValueError("dataset must be a single directory name")

    now = datetime.now(timezone.utc)
    run = raw_root / dataset / f"ingest_date={now:%Y-%m-%d}" / f"run_{now:%Y%m%dT%H%M%S%fZ}_{uuid.uuid4().hex[:8]}"
    run.mkdir(parents=True, exist_ok=False)
    destination = run / filename
    partial = run / (filename + ".part")
    manifest = {
        "dataset": dataset,
        "source": url,
        "ingested_at_utc": now.isoformat(),
        "bytes": 0,
        "sha256": None,
        "destination": str(destination.resolve()),
        "status": "failed",
    }
    digest = hashlib.sha256()

    try:
        request = Request(
            url,
            headers={"User-Agent": "BigDataCourseCheckpoint/1.0", "Accept": "application/octet-stream"},
        )
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

        with partial.open("rb") as stream:
            if stream.read(4) != b"PAR1":
                raise ValueError("Invalid Parquet header")
            stream.seek(-4, 2)
            if stream.read(4) != b"PAR1":
                raise ValueError("Invalid Parquet footer")

        import pyarrow.parquet as pq

        with pq.ParquetFile(partial) as parquet:
            if parquet.metadata.num_rows <= 0:
                raise ValueError("Parquet contains no rows")
            manifest["rows"] = parquet.metadata.num_rows

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
        (run / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        log = ROOT / "logs" / "ingest.jsonl"
        log.parent.mkdir(exist_ok=True)
        with log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(manifest, ensure_ascii=False) + "\n")
        print(json.dumps(manifest, ensure_ascii=False), flush=True)

    return destination
