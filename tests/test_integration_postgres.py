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
def test_warmdb_init_and_test_run(tmp_path: Path):
    """High-level integration test.

    Requires a reachable Postgres using env vars (see example_project/settings.py):
    - WARMDB_PGHOST, WARMDB_PGPORT, WARMDB_PGUSER, WARMDB_PGPASSWORD, WARMDB_PGDATABASE

    This test shells out to `python manage.py ...` to exercise the management command
    and runner in a realistic process.
    """

    env = os.environ.copy()

    # If unset, default to typical local docker-compose values.
    env.setdefault("WARMDB_PGHOST", "localhost")
    env.setdefault("WARMDB_PGPORT", "5432")
    env.setdefault("WARMDB_PGUSER", "postgres")
    env.setdefault("WARMDB_PGPASSWORD", "postgres")
    env.setdefault("WARMDB_PGDATABASE", "postgres")

    project_src = Path(__file__).resolve().parent / "example_project"
    project = tmp_path / "example_project"
    shutil.copytree(project_src, project)

    def run_manage(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "manage.py", *args],
            cwd=str(project),
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    init = run_manage("warmdb", "init", "--pool-size", "1", "--prefix", "warmdb_it")
    assert init.returncode == 0, init.stdout + "\n" + init.stderr

    test = run_manage("test", "example_app")
    assert test.returncode == 0, test.stdout + "\n" + test.stderr
