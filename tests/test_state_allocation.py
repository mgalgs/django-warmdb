from __future__ import annotations

import os
from pathlib import Path

from warmdb.state import (
    DBRow,
    STATUS_CONSUMED,
    STATUS_IN_USE,
    STATUS_READY,
    WarmDBState,
)


def test_allocate_ready_atomic(tmp_path: Path):
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    state.upsert_dbs(
        [
            DBRow(
                name="warmdb_x_01",
                status=STATUS_READY,
                allocated_to_pid=None,
                allocated_at=None,
                last_error=None,
                schema_hash="h",
            ),
            DBRow(
                name="warmdb_x_02",
                status=STATUS_READY,
                allocated_to_pid=None,
                allocated_at=None,
                last_error=None,
                schema_hash="h",
            ),
        ]
    )

    name = state.allocate_ready(pid=os.getpid())
    assert name in {"warmdb_x_01", "warmdb_x_02"}

    rows = {r.name: r for r in state.list_dbs()}
    assert rows[name].status == STATUS_IN_USE
    assert rows[name].allocated_to_pid == os.getpid()
    assert rows[name].allocated_at is not None


def test_reclaim_dead_pid(tmp_path: Path):
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    # PID 999999 should not exist on typical systems.
    state.upsert_dbs(
        [
            DBRow(
                name="warmdb_x_01",
                status=STATUS_IN_USE,
                allocated_to_pid=999999,
                allocated_at="2000-01-01T00:00:00+00:00",
                last_error=None,
                schema_hash="h",
            )
        ]
    )

    reclaimed = state.reclaim_stale_in_use(ttl_seconds=None)
    assert reclaimed == 1

    rows = state.list_dbs()
    assert rows[0].status == STATUS_READY
    assert rows[0].allocated_to_pid is None


def test_mark_consumed_clears_allocation(tmp_path: Path):
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    state.upsert_dbs(
        [
            DBRow(
                name="warmdb_x_01",
                status=STATUS_IN_USE,
                allocated_to_pid=os.getpid(),
                allocated_at="2000-01-01T00:00:00+00:00",
                last_error="oops",
                schema_hash="h",
            )
        ]
    )

    state.mark_consumed("warmdb_x_01")

    rows = state.list_dbs()
    assert rows[0].status == STATUS_CONSUMED
    assert rows[0].allocated_to_pid is None
    assert rows[0].allocated_at is None
    assert rows[0].last_error is None
