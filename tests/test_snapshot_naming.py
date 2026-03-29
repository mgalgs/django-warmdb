from __future__ import annotations

from warmdb.naming import snapshot_clone_db_name, snapshot_db_name


def test_snapshot_db_name():
    assert snapshot_db_name("20260328_1430") == "warmdb_snap_20260328_1430"


def test_snapshot_clone_db_name():
    assert (
        snapshot_clone_db_name("20260328_1430", "my-worktree")
        == "warmdb_snapclone_20260328_1430_my-worktree"
    )
