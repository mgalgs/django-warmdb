from __future__ import annotations

import datetime as _dt
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional


STATUS_INITIALIZING = "initializing"
STATUS_READY = "ready"
STATUS_IN_USE = "in-use"
STATUS_CONSUMED = "consumed"
STATUS_ERROR = "error"


@dataclass(frozen=True)
class DBRow:
    name: str
    status: str
    allocated_to_pid: int | None
    allocated_at: str | None
    last_error: str | None
    schema_hash: str


class WarmDBState:
    def __init__(self, path: Path):
        self.path = path

    def exists(self) -> bool:
        return self.path.exists()

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dbs (
                  name TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  allocated_to_pid INTEGER,
                  allocated_at TEXT,
                  last_error TEXT,
                  schema_hash TEXT NOT NULL
                );
                """
            )

    def get_meta(self, key: str) -> str | None:
        if not self.exists():
            return None
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return None if row is None else str(row["value"])

    def set_meta(self, key: str, value: str) -> None:
        self.ensure_schema()
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()

    def list_dbs(self) -> list[DBRow]:
        if not self.exists():
            return []
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT name, status, allocated_to_pid, allocated_at, last_error, schema_hash FROM dbs ORDER BY name"
            ).fetchall()
        return [
            DBRow(
                name=r["name"],
                status=r["status"],
                allocated_to_pid=r["allocated_to_pid"],
                allocated_at=r["allocated_at"],
                last_error=r["last_error"],
                schema_hash=r["schema_hash"],
            )
            for r in rows
        ]

    def upsert_dbs(self, rows: Iterable[DBRow]) -> None:
        self.ensure_schema()
        with self.connect() as conn:
            for r in rows:
                conn.execute(
                    """
                    INSERT INTO dbs(name, status, allocated_to_pid, allocated_at, last_error, schema_hash)
                    VALUES(?, ?, ?, ?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                      status=excluded.status,
                      allocated_to_pid=excluded.allocated_to_pid,
                      allocated_at=excluded.allocated_at,
                      last_error=excluded.last_error,
                      schema_hash=excluded.schema_hash
                    """,
                    (
                        r.name,
                        r.status,
                        r.allocated_to_pid,
                        r.allocated_at,
                        r.last_error,
                        r.schema_hash,
                    ),
                )

    def mark_ready(self, name: str) -> None:
        if not self.exists():
            return
        with self.connect() as conn:
            conn.execute(
                "UPDATE dbs SET status=?, allocated_to_pid=NULL, allocated_at=NULL, last_error=NULL WHERE name=?",
                (STATUS_READY, name),
            )

    def mark_consumed(self, name: str) -> None:
        if not self.exists():
            return
        with self.connect() as conn:
            conn.execute(
                "UPDATE dbs SET status=?, allocated_to_pid=NULL, allocated_at=NULL, last_error=NULL WHERE name=?",
                (STATUS_CONSUMED, name),
            )

    def mark_error(self, name: str, error: str) -> None:
        if not self.exists():
            return
        with self.connect() as conn:
            conn.execute(
                "UPDATE dbs SET status=?, last_error=? WHERE name=?",
                (STATUS_ERROR, error, name),
            )

    def _pid_alive(self, pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def reclaim_stale_in_use(self, *, ttl_seconds: int | None = 4 * 60 * 60) -> int:
        if not self.exists():
            return 0

        now = _dt.datetime.now(tz=_dt.timezone.utc)
        reclaimed = 0

        with self.connect() as conn:
            rows = conn.execute(
                "SELECT name, allocated_to_pid, allocated_at FROM dbs WHERE status=?",
                (STATUS_IN_USE,),
            ).fetchall()

            for r in rows:
                pid = r["allocated_to_pid"]
                allocated_at = r["allocated_at"]

                stale = False

                if pid is None or not self._pid_alive(int(pid)):
                    stale = True

                if not stale and ttl_seconds is not None and allocated_at:
                    try:
                        ts = _dt.datetime.fromisoformat(str(allocated_at))
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=_dt.timezone.utc)
                        if (now - ts).total_seconds() > ttl_seconds:
                            stale = True
                    except ValueError:
                        stale = True

                if stale:
                    conn.execute(
                        "UPDATE dbs SET status=?, allocated_to_pid=NULL, allocated_at=NULL WHERE name=?",
                        (STATUS_READY, r["name"]),
                    )
                    reclaimed += 1

        return reclaimed

    def allocate_ready(self, *, pid: int | None = None) -> str | None:
        """Atomically allocate a ready DB and mark it in-use."""

        if not self.exists():
            return None

        pid = os.getpid() if pid is None else pid
        allocated_at = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                # Best-effort cleanup while we hold the lock.
                self._reclaim_stale_in_use_locked(conn)

                row = conn.execute(
                    "SELECT name FROM dbs WHERE status=? ORDER BY name LIMIT 1",
                    (STATUS_READY,),
                ).fetchone()

                if row is None:
                    conn.execute("ROLLBACK")
                    return None

                name = str(row["name"])
                conn.execute(
                    "UPDATE dbs SET status=?, allocated_to_pid=?, allocated_at=? WHERE name=?",
                    (STATUS_IN_USE, int(pid), allocated_at, name),
                )
                conn.execute("COMMIT")
                return name
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _reclaim_stale_in_use_locked(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            "SELECT name, allocated_to_pid FROM dbs WHERE status=?",
            (STATUS_IN_USE,),
        ).fetchall()

        for r in rows:
            pid = r["allocated_to_pid"]
            if pid is None:
                conn.execute(
                    "UPDATE dbs SET status=?, allocated_to_pid=NULL, allocated_at=NULL WHERE name=?",
                    (STATUS_READY, r["name"]),
                )
                continue

            if not self._pid_alive(int(pid)):
                conn.execute(
                    "UPDATE dbs SET status=?, allocated_to_pid=NULL, allocated_at=NULL WHERE name=?",
                    (STATUS_READY, r["name"]),
                )
