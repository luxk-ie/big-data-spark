import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from src.pipeline_tlc import counts, demand, sha256, transformed, validate_manifest, validate_zones


class ContractTests(unittest.TestCase):
    def test_manifest_checks_status_size_hash_and_sibling(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            raw = folder / "yellow_tripdata_2025-01.parquet"
            raw.write_bytes(b"fixture")
            manifest = folder / "manifest.json"
            info = {"status": "success", "dataset": "nyc_tlc_yellow", "destination": str(folder / "old-machine" / raw.name),
                    "bytes": 7, "sha256": sha256(raw)}
            manifest.write_text(json.dumps(info))
            self.assertEqual(validate_manifest(manifest)[1], raw)
            for changed in ({"status": "failed"}, {"bytes": 8}, {"sha256": "0" * 64}):
                manifest.write_text(json.dumps(info | changed))
                with self.assertRaises(ValueError):
                    validate_manifest(manifest)

    def test_zone_csv_contract(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "zones.csv"
            path.write_text("LocationID,Borough,Zone,service_zone\n1,Queens,A,B\n")
            self.assertEqual(validate_zones(path)[1], 1)
            path.write_text("LocationID,Borough,Zone,service_zone\n1,Queens,A,B\n1,Bronx,C,D\n")
            with self.assertRaises(ValueError):
                validate_zones(path)
            path.write_text("LocationID,Borough,Zone,service_zone\n1,Queens,A,B\n01,Bronx,C,D\n")
            with self.assertRaises(ValueError):
                validate_zones(path)


class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("checkpoint3-tests").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_scope_unknown_join_and_grain(self):
        spark = self.spark
        raw = spark.sql("""SELECT * FROM VALUES
            ('2025-01-01 00:00:00','2025-01-01 00:10:00',1,1D,10D),
            ('2025-01-01 00:00:00','2025-01-01 00:10:00',1,1D,10D),
            ('2025-01-31 23:59:59',NULL,NULL,-1D,-2D),
            ('2025-01-02 01:00:00','2025-01-02 01:05:00',999,1D,10D),
            ('2025-02-01 00:00:00',NULL,1,1D,10D),
            (NULL,NULL,1,1D,10D)
            AS t(tpep_pickup_datetime,tpep_dropoff_datetime,PULocationID,trip_distance,total_amount)""")
        zones = spark.sql("SELECT 1 AS LocationID, 'Queens' AS Borough")
        facts = transformed(raw)
        joined, gold = demand(facts, zones)
        result = counts(facts, joined, gold, raw.count())
        self.assertEqual(result["raw_input_rows"], 6)
        self.assertEqual(result["invalid_pickup_timestamp_rows"], 1)
        self.assertEqual(result["out_of_scope_rows"], 1)
        self.assertEqual(result["in_scope_rows"], 4)
        self.assertEqual(result["trip_count"], 4)
        self.assertEqual(result["zone_status"], {"matched": 2, "null_key": 1, "unmatched": 1})
        self.assertEqual(result["invalid_distance_rows"], 1)
        self.assertEqual(result["invalid_amount_rows"], 1)
        self.assertEqual(gold.filter("pickup_borough = 'Queens'").first().trip_count, 2)
        duplicate_zones = spark.sql("SELECT * FROM VALUES (1,'Queens'),(1,'Bronx') AS t(LocationID,Borough)")
        with self.assertRaises(ValueError):
            demand(facts, duplicate_zones)


if __name__ == "__main__":
    unittest.main()
