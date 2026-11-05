"""Unit tests for the per-job scratch folder: what a dead job leaves is cleared, a running job's folder is not."""

from __future__ import annotations

import fcntl
import os
import tempfile
from pathlib import Path
from unittest import TestCase, main, mock

from fraudstream.jobs import spark_settings
from fraudstream.jobs.spark_settings import SCRATCH_LOCK_NAME, claim_scratch_folder


class ClaimScratchFolderTest(TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        lock_patch = mock.patch.object(spark_settings, "_scratch_lock", None)
        lock_patch.start()
        self.addCleanup(lock_patch.stop)
        # The folder is removed by the test's cleanup, not at interpreter exit.
        atexit_patch = mock.patch.object(spark_settings.atexit, "register")
        atexit_patch.start()
        self.addCleanup(atexit_patch.stop)

    def _run_folder(self, name: str) -> Path:
        folder = self.root / name
        folder.mkdir()
        (folder / SCRATCH_LOCK_NAME).touch()
        (folder / "shuffle.data").write_text("left behind", encoding="utf-8")
        return folder

    def _claim(self) -> Path:
        with mock.patch.dict(os.environ, {"SPARK_LOCAL_DIRS": str(self.root)}):
            claim_scratch_folder()
            self.addCleanup(self._release)
            return Path(os.environ["SPARK_LOCAL_DIRS"])

    def _release(self) -> None:
        if spark_settings._scratch_lock is not None:
            spark_settings._scratch_lock.close()

    def test_job_gets_its_own_folder_under_the_root(self) -> None:
        folder = self._claim()

        self.assertEqual(folder.parent, self.root)
        self.assertTrue(folder.name.startswith("run-"))
        self.assertTrue((folder / SCRATCH_LOCK_NAME).exists())

    def test_folder_of_a_dead_job_is_cleared(self) -> None:
        dead = self._run_folder("run-dead")

        self._claim()

        self.assertFalse(dead.exists())

    def test_folder_that_lost_its_lock_is_cleared(self) -> None:
        # A job killed while it deletes its own folder can leave the rest of it behind.
        leftover = self._run_folder("run-leftover")
        (leftover / SCRATCH_LOCK_NAME).unlink()

        self._claim()

        self.assertFalse(leftover.exists())

    def test_folder_of_a_running_job_is_kept(self) -> None:
        running = self._run_folder("run-running")
        with (running / SCRATCH_LOCK_NAME).open() as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self._claim()

            self.assertTrue((running / "shuffle.data").exists())

    def test_second_session_in_one_job_keeps_the_same_folder(self) -> None:
        with mock.patch.dict(os.environ, {"SPARK_LOCAL_DIRS": str(self.root)}):
            claim_scratch_folder()
            self.addCleanup(self._release)
            first = os.environ["SPARK_LOCAL_DIRS"]
            claim_scratch_folder()

            self.assertEqual(os.environ["SPARK_LOCAL_DIRS"], first)

    def test_nothing_happens_without_the_setting(self) -> None:
        with mock.patch.dict(os.environ, clear=True):
            claim_scratch_folder()

            self.assertNotIn("SPARK_LOCAL_DIRS", os.environ)


if __name__ == "__main__":
    main()
