from __future__ import annotations

from pathlib import Path

import pytest

from warmdb.state import WarmDBState


def _make_state(tmp_path: Path) -> WarmDBState:
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()
    state.ensure_snapshot_schema()
    return state


def test_add_and_get_snapshot(tmp_path: Path):
    state = _make_state(tmp_path)
    state.add_snapshot("warmdb_snap_test1", "test1", "mydb")

    snap = state.get_snapshot("test1")
    assert snap is not None
    assert snap.name == "warmdb_snap_test1"
    assert snap.label == "test1"
    assert snap.source_db == "mydb"
    assert snap.created_at is not None


def test_get_snapshot_not_found(tmp_path: Path):
    state = _make_state(tmp_path)
    assert state.get_snapshot("nonexistent") is None


def test_get_latest_snapshot(tmp_path: Path):
    state = _make_state(tmp_path)
    state.add_snapshot("warmdb_snap_a", "a", "mydb")
    state.add_snapshot("warmdb_snap_b", "b", "mydb")

    latest = state.get_latest_snapshot()
    assert latest is not None
    assert latest.label == "b"


def test_get_latest_snapshot_empty(tmp_path: Path):
    state = _make_state(tmp_path)
    assert state.get_latest_snapshot() is None


def test_list_snapshots(tmp_path: Path):
    state = _make_state(tmp_path)
    state.add_snapshot("warmdb_snap_a", "a", "mydb")
    state.add_snapshot("warmdb_snap_b", "b", "mydb")

    snapshots = state.list_snapshots()
    assert len(snapshots) == 2
    assert snapshots[0].label == "a"
    assert snapshots[1].label == "b"


def test_remove_snapshot(tmp_path: Path):
    state = _make_state(tmp_path)
    state.add_snapshot("warmdb_snap_a", "a", "mydb")
    state.remove_snapshot("a")
    assert state.get_snapshot("a") is None
    assert state.list_snapshots() == []


def test_add_and_get_clone(tmp_path: Path):
    state = _make_state(tmp_path)
    state.add_snapshot("warmdb_snap_a", "a", "mydb")
    state.add_clone("warmdb_snapclone_a_wt1", "a", "wt1")

    clone = state.get_clone("wt1")
    assert clone is not None
    assert clone.name == "warmdb_snapclone_a_wt1"
    assert clone.snapshot_label == "a"
    assert clone.clone_label == "wt1"


def test_get_clone_not_found(tmp_path: Path):
    state = _make_state(tmp_path)
    assert state.get_clone("nonexistent") is None


def test_list_clones_by_snapshot(tmp_path: Path):
    state = _make_state(tmp_path)
    state.add_snapshot("warmdb_snap_a", "a", "mydb")
    state.add_snapshot("warmdb_snap_b", "b", "mydb")
    state.add_clone("warmdb_snapclone_a_wt1", "a", "wt1")
    state.add_clone("warmdb_snapclone_a_wt2", "a", "wt2")
    state.add_clone("warmdb_snapclone_b_wt3", "b", "wt3")

    clones_a = state.list_clones(snapshot_label="a")
    assert len(clones_a) == 2
    assert {c.clone_label for c in clones_a} == {"wt1", "wt2"}

    clones_b = state.list_clones(snapshot_label="b")
    assert len(clones_b) == 1

    all_clones = state.list_clones()
    assert len(all_clones) == 3


def test_remove_clone(tmp_path: Path):
    state = _make_state(tmp_path)
    state.add_snapshot("warmdb_snap_a", "a", "mydb")
    state.add_clone("warmdb_snapclone_a_wt1", "a", "wt1")
    state.remove_clone("wt1")
    assert state.get_clone("wt1") is None
    assert state.list_clones() == []


def test_duplicate_snapshot_label_raises(tmp_path: Path):
    state = _make_state(tmp_path)
    state.add_snapshot("warmdb_snap_a", "a", "mydb")
    with pytest.raises(Exception):
        state.add_snapshot("warmdb_snap_a2", "a", "mydb")


def test_duplicate_clone_label_raises(tmp_path: Path):
    state = _make_state(tmp_path)
    state.add_snapshot("warmdb_snap_a", "a", "mydb")
    state.add_clone("warmdb_snapclone_a_wt1", "a", "wt1")
    with pytest.raises(Exception):
        state.add_clone("warmdb_snapclone_a_wt1_dup", "a", "wt1")
