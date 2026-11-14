"""Run a local PySpark smoke test and append evidence to logs/env_check.txt."""

import logging
from pathlib import Path
import time
import platform
import subprocess


def main() -> int:
    log_path = Path(__file__).resolve().parents[1] / "logs" / "env_check.txt"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()],
    )
    logging.Formatter.converter = time.gmtime
    logger = logging.getLogger(__name__)
    spark = None
    exit_code = 0
    started = time.perf_counter()
    logger.info("Starting PySpark environment check; master=local[*]")
    logger.info("Python version=%s", platform.python_version())

    try:
        java = subprocess.run(["java", "-version"], capture_output=True, text=True, check=True)
        logger.info("Java version: %s", (java.stderr or java.stdout).strip())
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[*]")
            .appName("EnvironmentSmokeTest")
            .getOrCreate()
        )
        logger.info("Spark version=%s; master=%s", spark.version, spark.sparkContext.master)
        expected = 1_000_000
        actual = spark.range(expected).count()
        logger.info("Row count: expected=%s; actual=%s", expected, actual)
        if actual != expected:
            raise RuntimeError(f"Expected {expected} rows, got {actual}")
    except Exception:
        logger.exception("Environment check FAILED")
        exit_code = 1
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                logger.exception("Failed to stop SparkSession")
                exit_code = 1

    logger.info(
        "Environment check %s; elapsed_seconds=%.3f",
        "PASSED" if exit_code == 0 else "FAILED",
        time.perf_counter() - started,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
