"""What every Spark job shares: its `--spark-conf` and `--retain-ui` flags, and its temp folder.

`--spark-conf KEY=VALUE` keeps a run's settings outside the code.

A baseline run and a tuned run use the same job; only their settings differ. Each
setting is applied last, after the job's own defaults, so it always wins, and the
job writes what it applied into its summary.

    PYTHONPATH=src uv run --extra spark --extra storage python -m fraudstream.jobs.silver.transactions --spark-conf spark.sql.adaptive.enabled=false --spark-conf spark.sql.shuffle.partitions=200
"""

from __future__ import annotations

import argparse
import atexit
import fcntl
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import IO, Any, Sequence

SCRATCH_LOCK_NAME = ".lock"

# Held open for the life of the job. The kernel lets go of it however the process dies.
_scratch_lock: IO[str] | None = None


def add_spark_conf_argument(parser: argparse.ArgumentParser) -> None:
    """Add the repeatable `--spark-conf KEY=VALUE` flag to one job parser."""

    parser.add_argument(
        "--spark-conf",
        action="append",
        default=[],
        type=_checked_pair,
        metavar="KEY=VALUE",
        help="A Spark setting applied after the job's defaults; repeat for several.",
    )


def parse_spark_conf(values: Sequence[str]) -> dict[str, str]:
    """Turn `KEY=VALUE` strings into settings. Spark ignores unknown keys silently, so non-`spark.` keys are refused."""

    conf: dict[str, str] = {}
    for value in values:
        key, separator, setting = value.partition("=")
        key = key.strip()
        if not separator:
            raise ValueError(f"--spark-conf needs KEY=VALUE, got {value!r}")
        if not key.startswith("spark."):
            raise ValueError(f"--spark-conf keys start with 'spark.', got {key!r}")
        conf[key] = setting.strip()
    return conf


def _checked_pair(value: str) -> str:
    """Reject a bad pair while parsing, so the job stops with a usage error before Spark starts."""

    try:
        parse_spark_conf([value])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return value


def apply_spark_conf(builder: Any, conf: dict[str, str]) -> Any:
    """Set each setting on a SparkSession builder; call it last so it overrides the job's defaults."""

    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder


def add_retain_ui_argument(parser: argparse.ArgumentParser) -> None:
    """Add the `--retain-ui SECONDS` flag to one job parser."""

    parser.add_argument(
        "--retain-ui",
        type=_seconds,
        default=0,
        metavar="SECONDS",
        help="Turn the Spark UI on and keep it open this many seconds after the job finishes.",
    )


def _seconds(value: str) -> int:
    seconds = int(value)
    if seconds < 0:
        raise argparse.ArgumentTypeError("--retain-ui must be zero or more")
    return seconds


def configure_spark_builder(builder: Any, retain_ui: int) -> Any:
    """Give the job its temp folder, and turn the Spark UI on only when it was asked for."""

    claim_scratch_folder()
    return builder.config("spark.ui.enabled", str(retain_ui > 0).lower())


def keep_ui_open(spark: Any, seconds: int) -> None:
    """Hold the finished job's Spark UI open for inspection."""

    if seconds:
        print(f"Job finished. Spark UI stays open for {seconds} seconds.", file=sys.stderr, flush=True)
        time.sleep(seconds)


def _held_by_running_job(folder: Path) -> bool:
    """A folder with no lock file belongs to no one: its job died while deleting it."""

    try:
        with (folder / SCRATCH_LOCK_NAME).open() as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    except FileNotFoundError:
        return False
    return False


def claim_scratch_folder() -> None:
    """Give this job its own folder under SPARK_LOCAL_DIRS, and clear the ones dead jobs left behind.

    Spark only deletes its shuffle and cache files on a clean exit, so a job that runs
    out of memory leaves them on the volume, and every retry adds another copy. A job's
    folder goes with it on any exit Python survives. After a hard kill the lock is what
    tells the next job that the folder's owner is gone, and a running job is never touched.

    LOCK_EX	            Request an exclusive lock
    LOCK_NB	            Don't wait if someone else already holds the lock
    BlockingIOError	    Another process currently holds the lock
    """


    global _scratch_lock
    root = os.environ.get("SPARK_LOCAL_DIRS")
    if root is None or _scratch_lock is not None:
        return
    for folder in Path(root).glob("run-*"):
        if not _held_by_running_job(folder):
            shutil.rmtree(folder, ignore_errors=True)
    # It only gets its run- name once locked, so a job starting at the same moment never clears it.
    starting_folder = Path(tempfile.mkdtemp(prefix="starting-", dir=root))
    _scratch_lock = (starting_folder / SCRATCH_LOCK_NAME).open("w")
    fcntl.flock(_scratch_lock, fcntl.LOCK_EX)
    run_folder = str(starting_folder.rename(starting_folder.with_name(starting_folder.name.replace("starting-", "run-", 1))))
    os.environ["SPARK_LOCAL_DIRS"] = run_folder
    atexit.register(shutil.rmtree, run_folder, ignore_errors=True)
