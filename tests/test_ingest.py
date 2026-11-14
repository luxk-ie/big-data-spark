import hashlib
import json
from contextlib import redirect_stdout
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import io
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ingest_http import ingest


DATASET = "nyc_tlc_yellow"
FILENAME = "yellow_tripdata_2025-01.parquet"


class Response(io.BytesIO):
    headers = {"Content-Type": "application/octet-stream"}
    url = "https://example.test/yellow_tripdata_fixture.parquet"


def parquet_bytes(rows=2):
    output = pa.BufferOutputStream()
    pq.write_table(pa.table({"id": list(range(rows))}), output)
    return output.getvalue().to_pybytes()


def ingest_silently(*args):
    """Keep fixture manifests out of the unittest evidence stream."""
    with redirect_stdout(io.StringIO()):
        return ingest(*args)


class IngestTests(unittest.TestCase):
    def test_three_runs_preserve_bytes(self):
        raw = parquet_bytes()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("ingest_http.ROOT", root), patch("ingest_http.urlopen", side_effect=lambda *a, **k: Response(raw)):
                paths = [ingest_silently(Response.url, root / "raw", DATASET, FILENAME) for _ in range(3)]
            self.assertEqual(len(set(paths)), 3)
            for path in paths:
                self.assertEqual(path.read_bytes(), raw)
                meta = json.loads(path.with_name("manifest.json").read_text())
                self.assertEqual(meta["sha256"], hashlib.sha256(raw).hexdigest())
                self.assertEqual(meta["bytes"], len(raw))
                self.assertEqual(meta["status"], "success")

    def test_invalid_or_empty_response_fails(self):
        for raw in [b'', b'<html>Error</html>', b'not parquet']:
            with self.subTest(raw=raw), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with patch("ingest_http.ROOT", root), patch("ingest_http.urlopen", return_value=Response(raw)):
                    with self.assertRaises(ValueError):
                        ingest_silently(Response.url, root / "raw", DATASET, FILENAME)
                manifest = next((root / "raw").rglob("manifest.json"))
                self.assertEqual(json.loads(manifest.read_text())["status"], "failed")
                self.assertFalse(manifest.with_name(FILENAME).exists())

    def test_parquet_is_closed_before_rename(self):
        raw = parquet_bytes()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("ingest_http.ROOT", root), patch("ingest_http.urlopen", return_value=Response(raw)):
                path = ingest_silently(Response.url, root / "raw", DATASET, FILENAME)
            self.assertEqual(path.read_bytes(), raw)
            self.assertEqual(json.loads(path.with_name("manifest.json").read_text())["rows"], 2)

    def test_network_failure_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("ingest_http.ROOT", root), patch("ingest_http.urlopen", side_effect=TimeoutError("fixture timeout")):
                with self.assertRaises(TimeoutError):
                    ingest_silently(Response.url, root / "raw", DATASET, FILENAME)
            manifest = next((root / "raw").rglob("manifest.json"))
            meta = json.loads(manifest.read_text())
            self.assertEqual(meta["status"], "failed")
            self.assertIsNone(meta["sha256"])
            self.assertIn("TimeoutError", meta["error"])

    def test_corrupt_and_empty_parquet_fail(self):
        for raw in [b"not parquet", b"PAR1invalidPAR1", parquet_bytes(rows=0)]:
            with self.subTest(size=len(raw)), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with patch("ingest_http.ROOT", root), patch("ingest_http.urlopen", return_value=Response(raw)):
                    with self.assertRaises(Exception):
                        ingest_silently(Response.url, root / "raw", DATASET, FILENAME)
                manifest = next((root / "raw").rglob("manifest.json"))
                self.assertEqual(json.loads(manifest.read_text())["status"], "failed")
                self.assertFalse(manifest.with_name(FILENAME).exists())


if __name__ == "__main__":
    unittest.main()
