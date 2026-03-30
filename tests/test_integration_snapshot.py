from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


def _integration_enabled() -> bool:
    return os.environ.get("WARMDB_INTEGRATION") == "1"


@pytest.mark.integration
@pytest.mark.skipif(
    not _integration_enabled(), reason="Set WARMDB_INTEGRATION=1 to enable"
)
class TestSnapshotIntegration:
    """Integration tests for snapshot commands.

    Requires a reachable Postgres using env vars:
    WARMDB_PGHOST, WARMDB_PGPORT, WARMDB_PGUSER, WARMDB_PGPASSWORD, WARMDB_PGDATABASE
    """

    @pytest.fixture(autouse=True)
    def setup_project(self, tmp_path: Path):
        self.env = os.environ.copy()
        self.env.setdefault("WARMDB_PGHOST", "localhost")
        self.env.setdefault("WARMDB_PGPORT", "5432")
        self.env.setdefault("WARMDB_PGUSER", "postgres")
        self.env.setdefault("WARMDB_PGPASSWORD", "postgres")
        self.env.setdefault("WARMDB_PGDATABASE", "postgres")

        project_src = Path(__file__).resolve().parent / "example_project"
        self.project = tmp_path / "example_project"
        shutil.copytree(project_src, self.project)

        # Create a source database with migrations applied for snapshot tests.
        # We use warmdb init to create a migrated template, then use that as
        # our "source" DB. But for snapshot create, we need settings.DATABASES
        # to point to a real DB. The default WARMDB_PGDATABASE works — we just
        # need migrations applied to it first.
        self._run("warmdb", "init", "--pool-size", "1", "--prefix", "warmdb_snap_it")

        yield

        # Clean up: invalidate pool and drop any snapshot databases
        self._run("warmdb", "snapshot", "prune", "--keep", "0", "--cascade")
        self._run("warmdb", "invalidate")

    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "manage.py", *args],
            cwd=str(self.project),
            env=self.env,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_snapshot_create_list_drop(self):
        # Create a snapshot
        result = self._run("warmdb", "snapshot", "create", "--label", "test1")
        assert result.returncode == 0, result.stdout + "\n" + result.stderr
        assert "test1" in result.stdout

        # List snapshots
        result = self._run("warmdb", "snapshot", "list")
        assert result.returncode == 0, result.stdout + "\n" + result.stderr
        assert "test1" in result.stdout

        # Status
        result = self._run("warmdb", "snapshot", "status")
        assert result.returncode == 0, result.stdout + "\n" + result.stderr
        assert "test1" in result.stdout

        # Drop snapshot
        result = self._run("warmdb", "snapshot", "drop", "test1")
        assert result.returncode == 0, result.stdout + "\n" + result.stderr

        # List should be empty
        result = self._run("warmdb", "snapshot", "list")
        assert result.returncode == 0
        assert "No snapshots" in result.stdout

    def test_snapshot_clone_and_drop_clone(self):
        # Create snapshot
        result = self._run("warmdb", "snapshot", "create", "--label", "forclone")
        assert result.returncode == 0, result.stdout + "\n" + result.stderr

        # Clone it
        result = self._run("warmdb", "snapshot", "clone", "my-worktree")
        assert result.returncode == 0, result.stdout + "\n" + result.stderr
        assert "warmdb_snapclone_forclone_my-worktree" in result.stdout

        # Drop the clone
        result = self._run("warmdb", "snapshot", "drop-clone", "my-worktree")
        assert result.returncode == 0, result.stdout + "\n" + result.stderr

        # Clean up snapshot
        result = self._run("warmdb", "snapshot", "drop", "forclone")
        assert result.returncode == 0

    def test_drop_snapshot_with_clones_requires_cascade(self):
        self._run("warmdb", "snapshot", "create", "--label", "hasclones")
        self._run("warmdb", "snapshot", "clone", "wt1", "--snapshot", "hasclones")

        # Drop without --cascade should fail
        result = self._run("warmdb", "snapshot", "drop", "hasclones")
        assert result.returncode != 0

        # Drop with --cascade should succeed
        result = self._run("warmdb", "snapshot", "drop", "hasclones", "--cascade")
        assert result.returncode == 0, result.stdout + "\n" + result.stderr

    def test_clone_from_nonexistent_snapshot_errors(self):
        result = self._run(
            "warmdb", "snapshot", "clone", "wt1", "--snapshot", "nonexistent"
        )
        assert result.returncode != 0

    def test_clone_idempotent(self):
        """Calling clone twice with the same label reuses the existing clone."""
        self._run("warmdb", "snapshot", "create", "--label", "idempotent")

        r1 = self._run(
            "warmdb", "snapshot", "clone", "wt-idem", "--snapshot", "idempotent"
        )
        assert r1.returncode == 0, r1.stdout + "\n" + r1.stderr

        r2 = self._run(
            "warmdb", "snapshot", "clone", "wt-idem", "--snapshot", "idempotent"
        )
        assert r2.returncode == 0, r2.stdout + "\n" + r2.stderr

        # Both should output the same DB name
        assert r1.stdout.strip() == r2.stdout.strip()

        # Clean up
        self._run("warmdb", "snapshot", "drop", "idempotent", "--cascade")

    def test_prune_keeps_recent(self):
        self._run("warmdb", "snapshot", "create", "--label", "old1")
        self._run("warmdb", "snapshot", "create", "--label", "old2")
        self._run("warmdb", "snapshot", "create", "--label", "latest")

        result = self._run("warmdb", "snapshot", "prune", "--keep", "1")
        assert result.returncode == 0, result.stdout + "\n" + result.stderr

        # Only latest should remain
        result = self._run("warmdb", "snapshot", "list")
        assert "latest" in result.stdout
        assert "old1" not in result.stdout
        assert "old2" not in result.stdout
