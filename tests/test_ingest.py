import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import io
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ingest_311 import ingest


class Response(io.BytesIO):
    headers = {"Content-Type": "text/csv"}
    url = "https://example.test/fixture.csv"


class IngestTests(unittest.TestCase):
    def test_three_runs_preserve_bytes(self):
        raw = b'unique_key,created_date\n1,2025-01-01T12:00:00\n'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("ingest_311.ROOT", root), patch("ingest_311.urlopen", side_effect=lambda *a, **k: Response(raw)):
                paths = [ingest(Response.url, root / "raw") for _ in range(3)]
            self.assertEqual(len(set(paths)), 3)
            for path in paths:
                self.assertEqual(path.read_bytes(), raw)
                meta = json.loads(path.with_name("manifest.json").read_text())
                self.assertEqual(meta["sha256"], hashlib.sha256(raw).hexdigest())
                self.assertEqual(meta["bytes"], len(raw))
                self.assertEqual(meta["status"], "success")

    def test_invalid_or_empty_response_fails(self):
        for raw in [b'', b'<html>Error</html>', b'unique_key,created_date\n']:
            with self.subTest(raw=raw), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with patch("ingest_311.ROOT", root), patch("ingest_311.urlopen", return_value=Response(raw)):
                    with self.assertRaises(ValueError):
                        ingest(Response.url, root / "raw")
                manifest = next((root / "raw").rglob("manifest.json"))
                self.assertEqual(json.loads(manifest.read_text())["status"], "failed")
                self.assertFalse(manifest.with_name("source.csv").exists())

    def test_parquet_is_closed_before_rename(self):
        output = pa.BufferOutputStream()
        pq.write_table(pa.table({"id": [1, 2]}), output)
        raw = output.getvalue().to_pybytes()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("ingest_311.ROOT", root), patch("ingest_311.urlopen", return_value=Response(raw)):
                path = ingest(Response.url, root / "raw", "taxi", "taxi.parquet")
            self.assertEqual(path.read_bytes(), raw)
            self.assertEqual(json.loads(path.with_name("manifest.json").read_text())["rows"], 2)

    def test_network_failure_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("ingest_311.ROOT", root), patch("ingest_311.urlopen", side_effect=TimeoutError("fixture timeout")):
                with self.assertRaises(TimeoutError):
                    ingest(Response.url, root / "raw")
            manifest = next((root / "raw").rglob("manifest.json"))
            meta = json.loads(manifest.read_text())
            self.assertEqual(meta["status"], "failed")
            self.assertIsNone(meta["sha256"])
            self.assertIn("TimeoutError", meta["error"])

    def test_corrupt_and_empty_parquet_fail(self):
        empty = pa.BufferOutputStream()
        pq.write_table(pa.table({"id": pa.array([], type=pa.int64())}), empty)
        for raw in [b"not parquet", b"PAR1invalidPAR1", empty.getvalue().to_pybytes()]:
            with self.subTest(size=len(raw)), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with patch("ingest_311.ROOT", root), patch("ingest_311.urlopen", return_value=Response(raw)):
                    with self.assertRaises(Exception):
                        ingest(Response.url, root / "raw", "taxi", "taxi.parquet")
                manifest = next((root / "raw").rglob("manifest.json"))
                self.assertEqual(json.loads(manifest.read_text())["status"], "failed")
                self.assertFalse(manifest.with_name("taxi.parquet").exists())


if __name__ == "__main__":
    unittest.main()
