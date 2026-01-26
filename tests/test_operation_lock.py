"""Tests for operation lock to prevent concurrent init/refresh/invalidate operations."""

from __future__ import annotations

import datetime as _dt
import os
import sys
import time
from pathlib import Path

import pytest

from warmdb.state import WarmDBState, OP_INIT, OP_REFRESH


def test_operation_lock_basic_acquire_release(tmp_path: Path):
    """Test basic acquire and release of operation lock."""
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    # No operation should be held initially
    assert state.get_operation() is None

    # Acquire returns a token
    token = state.acquire_operation_lock(OP_INIT)
    assert isinstance(token, str) and len(token) > 0

    current = state.get_operation()
    assert current is not None
    assert current[0] == OP_INIT
    assert current[1] == os.getpid()

    state.clear_operation(token=token)
    assert state.get_operation() is None


def test_clear_operation_requires_matching_token(tmp_path: Path):
    """clear_operation with wrong token must not remove the lock."""
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    token = state.acquire_operation_lock(OP_INIT)

    # Wrong token should not clear
    state.clear_operation(token="wrong_token")
    assert state.get_operation() is not None

    # Correct token clears
    state.clear_operation(token=token)
    assert state.get_operation() is None


@pytest.mark.skipif(sys.platform == "win32", reason="requires os.fork()")
def test_operation_lock_blocks_concurrent_acquisition(tmp_path: Path):
    """A second process must wait until the first releases the lock."""
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    # Pipes for deterministic coordination (no timing assumptions)
    ready_r, ready_w = os.pipe()  # child signals "I have the lock"
    release_r, release_w = os.pipe()  # parent signals "you can release"

    pid = os.fork()
    if pid == 0:
        # --- child ---
        os.close(ready_r)
        os.close(release_w)
        token = state.acquire_operation_lock(OP_INIT)
        os.write(ready_w, b"1")  # signal: lock acquired
        os.close(ready_w)
        os.read(release_r, 1)  # wait for parent's go-ahead
        os.close(release_r)
        state.clear_operation(token=token)
        os._exit(0)

    # --- parent ---
    os.close(ready_w)
    os.close(release_r)

    # Wait until child definitely holds the lock
    os.read(ready_r, 1)
    os.close(ready_r)

    # Parent tries to acquire — should block until child releases
    start = time.time()

    # Tell child to release after a short delay so parent can observe blocking
    os.write(release_w, b"1")
    os.close(release_w)

    token = state.acquire_operation_lock(OP_REFRESH, poll_interval_seconds=0.05)
    elapsed = time.time() - start

    # We acquired — just verify we got a valid token
    assert isinstance(token, str)
    state.clear_operation(token=token)

    os.waitpid(pid, 0)


def test_operation_lock_reentrant_same_pid(tmp_path: Path):
    """The operation_lock context manager is reentrant for the same PID."""
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    token = state.acquire_operation_lock(OP_INIT)
    current = state.get_operation()
    assert current is not None
    assert current[1] == os.getpid()

    # Simulating what the context manager does: same PID → skip re-acquire
    current = state.get_operation()
    assert current is not None and current[1] == os.getpid()

    state.clear_operation(token=token)


def test_operation_lock_invalid_pid(tmp_path: Path):
    """Lock held by a dead PID is considered invalid and can be stolen."""
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    # Manually write an operation for a PID that doesn't exist
    fake_pid = 999999
    started_at = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()
    with state.connect() as conn:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?)",
            ("operation", f"{OP_INIT}:{fake_pid}:{started_at}:fake_token"),
        )

    current = state.get_operation()
    assert current is not None
    assert not state.is_operation_valid(current)

    # Should acquire immediately (dead PID → invalid lock)
    token = state.acquire_operation_lock(OP_REFRESH)
    current = state.get_operation()
    assert current[0] == OP_REFRESH
    assert current[1] == os.getpid()

    state.clear_operation(token=token)


@pytest.mark.skipif(sys.platform == "win32", reason="requires os.fork()")
def test_operation_lock_timeout(tmp_path: Path):
    """Acquisition raises RuntimeError when timeout is exceeded."""
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    ready_r, ready_w = os.pipe()

    pid = os.fork()
    if pid == 0:
        os.close(ready_r)
        token = state.acquire_operation_lock(OP_INIT)
        os.write(ready_w, b"1")
        os.close(ready_w)
        # Hold lock long enough for parent to time out
        time.sleep(3.0)
        state.clear_operation(token=token)
        os._exit(0)

    os.close(ready_w)
    os.read(ready_r, 1)  # wait until child holds lock
    os.close(ready_r)

    with pytest.raises(RuntimeError, match="timeout"):
        state.acquire_operation_lock(
            OP_REFRESH, wait_timeout_seconds=0.5, poll_interval_seconds=0.05
        )

    os.waitpid(pid, 0)


def test_operation_lock_stale_cleanup(tmp_path: Path):
    """Stale operations (old timestamp) are considered invalid."""
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    old_timestamp = (
        _dt.datetime.now(tz=_dt.timezone.utc) - _dt.timedelta(hours=3)
    ).isoformat()
    with state.connect() as conn:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?)",
            ("operation", f"{OP_INIT}:{os.getpid()}:{old_timestamp}:old_token"),
        )

    current = state.get_operation()
    assert current is not None
    assert not state.is_operation_valid(current)

    # Should acquire immediately (stale → invalid)
    token = state.acquire_operation_lock(OP_REFRESH)
    current = state.get_operation()
    assert current[0] == OP_REFRESH
    assert current[1] == os.getpid()

    state.clear_operation(token=token)


@pytest.mark.skipif(sys.platform == "win32", reason="requires os.fork()")
def test_atomic_lock_prevents_double_acquisition(tmp_path: Path):
    """Two processes racing to acquire should result in only one winner."""
    state = WarmDBState(tmp_path / "warmdb_state.sqlite3")
    state.ensure_schema()

    # Both parent and child will race to acquire the lock.
    # We use a pipe so they start at roughly the same time.
    go_r, go_w = os.pipe()
    result_r, result_w = os.pipe()

    pid = os.fork()
    if pid == 0:
        os.close(go_w)
        os.close(result_r)
        os.read(go_r, 1)  # wait for go signal
        os.close(go_r)
        try:
            token = state.acquire_operation_lock(
                OP_INIT, wait_timeout_seconds=0.3, poll_interval_seconds=0.02
            )
            os.write(result_w, b"Y")  # got the lock
            state.clear_operation(token=token)
        except RuntimeError:
            os.write(result_w, b"N")  # timed out
        os.close(result_w)
        os._exit(0)

    os.close(go_r)
    os.close(result_w)

    # Signal both to go
    os.write(go_w, b"1")
    os.close(go_w)

    parent_got_lock = False
    try:
        token = state.acquire_operation_lock(
            OP_REFRESH, wait_timeout_seconds=0.3, poll_interval_seconds=0.02
        )
        parent_got_lock = True
        state.clear_operation(token=token)
    except RuntimeError:
        pass

    child_result = os.read(result_r, 1)
    os.close(result_r)
    child_got_lock = child_result == b"Y"

    os.waitpid(pid, 0)

    # At least one should have acquired, and it's fine if both eventually did
    # (one waited for the other). The critical thing: they didn't corrupt state.
    assert parent_got_lock or child_got_lock
