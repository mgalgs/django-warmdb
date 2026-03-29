# Plan: Snapshot Feature for Worktree Database Isolation

## Motivation

warmdb currently provides pools of **empty, freshly-migrated** databases for fast
test runs. A new use case: **git worktree isolation** — each worktree gets its own
Postgres database that is a clone of the developer's real local database (with data).

Snapshots are **manual** — no automatic schema hashing or invalidation. The developer
creates a snapshot when they want (after migrations, after loading fixture data, etc.)
and new worktrees clone from the latest snapshot. This keeps the system simple and
predictable.

## Design

### Concepts

- **Snapshot**: A Postgres database created via `CREATE DATABASE ... TEMPLATE <source_db>`.
  It is a frozen copy of the source DB at a point in time. Named
  `warmdb_snap_<label>` where `<label>` is user-provided or auto-generated
  (e.g. timestamp-based like `20260328_1430`).
- **Snapshot clone**: A Postgres database cloned from a snapshot via
  `CREATE DATABASE ... TEMPLATE <snapshot>`. Named `warmdb_snapclone_<label>_<clone_name>`
  where `<clone_name>` is user-provided (typically the worktree name).

### State Storage

Add two new tables to the existing `warmdb_state.sqlite3` (coexists with the
test pool tables `meta` and `dbs`):

```sql
CREATE TABLE IF NOT EXISTS snapshots (
  name TEXT PRIMARY KEY,          -- PG database name (warmdb_snap_<label>)
  label TEXT NOT NULL UNIQUE,     -- user-facing label
  source_db TEXT NOT NULL,        -- source database name at time of snapshot
  created_at TEXT NOT NULL        -- ISO timestamp
);

CREATE TABLE IF NOT EXISTS snapshot_clones (
  name TEXT PRIMARY KEY,          -- PG database name
  snapshot_label TEXT NOT NULL,   -- which snapshot this was cloned from
  clone_label TEXT NOT NULL,      -- user-provided label (e.g. worktree name)
  created_at TEXT NOT NULL,       -- ISO timestamp
  FOREIGN KEY (snapshot_label) REFERENCES snapshots(label)
);
```

### Management Commands

All under `manage.py warmdb snapshot <subcommand>`:

#### `snapshot create [--label <label>]`

1. Determine the source DB name from `settings.DATABASES["default"]["NAME"]`.
2. Generate label if not provided: `YYYYMMDD_HHMM` format.
3. Generate PG database name: `warmdb_snap_<label>`.
4. Call `terminate_sessions(alias, source_db)` to disconnect from the source DB.
   - Print a warning that active connections will be terminated.
5. `CREATE DATABASE <snapshot_name> TEMPLATE <source_db>`.
6. Record in `snapshots` table.
7. Print success with the snapshot label and name.

**Note**: `CREATE DATABASE ... TEMPLATE` requires exclusive access to the source.
The `terminate_sessions` helper in `postgres.py` already handles this. The
developer should stop their dev server before creating a snapshot. The command
should warn about this.

#### `snapshot list`

List all snapshots with label, source DB, and creation timestamp.
Also show clone count per snapshot.

#### `snapshot clone <clone_label> [--snapshot <label>]`

1. If `--snapshot` is not provided, use the most recent snapshot.
2. If no snapshots exist, error with a helpful message.
3. Generate PG database name: `warmdb_snapclone_<snapshot_label>_<clone_label>`.
4. Terminate sessions on the snapshot DB (required for TEMPLATE).
5. `CREATE DATABASE <clone_name> TEMPLATE <snapshot_db>`.
6. Record in `snapshot_clones` table.
7. Print the clone database name to stdout (for scripting).

#### `snapshot drop-clone <clone_label>`

1. Look up the clone by label.
2. Drop the Postgres database.
3. Remove from `snapshot_clones` table.

#### `snapshot drop <label> [--cascade]`

1. Check for existing clones of this snapshot.
2. If clones exist and `--cascade` not given, error listing the clones.
3. If `--cascade`, drop all clones first, then the snapshot.
4. Drop the Postgres database.
5. Remove from `snapshots` table.

#### `snapshot prune [--keep <N>]`

1. List snapshots ordered by `created_at`.
2. Keep the most recent N (default 1).
3. For older snapshots: drop them (cascade clones that no longer have a
   corresponding worktree directory — see clone metadata below).
4. Warn (don't drop) snapshots that still have active clones unless `--cascade`.

#### `snapshot status`

Show current state: all snapshots, their clones, and which ones are active.
This is the snapshot equivalent of `warmdb status`.

### Implementation Files

All new code goes in `src/warmdb/`:

1. **`snapshot.py`** (new) — Core snapshot logic:
   - `create_snapshot(alias, label, source_db, log)`
   - `clone_snapshot(alias, clone_label, snapshot_label, log)`
   - `drop_clone(alias, clone_label, log)`
   - `drop_snapshot(alias, label, cascade, log)`
   - `list_snapshots()`
   - `list_clones(snapshot_label=None)`
   - `prune_snapshots(keep, cascade, log)`

2. **`state.py`** — Add methods to `WarmDBState`:
   - `ensure_snapshot_schema()` — create the two new tables
   - `add_snapshot(name, label, source_db)`
   - `get_snapshot(label)` / `get_latest_snapshot()`
   - `list_snapshots()` / `remove_snapshot(label)`
   - `add_clone(name, snapshot_label, clone_label)`
   - `get_clone(clone_label)` / `list_clones(snapshot_label=None)`
   - `remove_clone(clone_label)`

3. **`naming.py`** — Add:
   - `snapshot_db_name(label)` → `warmdb_snap_<label>`
   - `snapshot_clone_db_name(snapshot_label, clone_label)` → `warmdb_snapclone_<snapshot_label>_<clone_label>`

4. **`management/commands/warmdb.py`** — Add `snapshot` subcommand with its own
   sub-subcommands. Use argparse sub-parsers:
   ```
   manage.py warmdb snapshot create [--label LABEL]
   manage.py warmdb snapshot list
   manage.py warmdb snapshot clone <clone_label> [--snapshot LABEL]
   manage.py warmdb snapshot drop-clone <clone_label>
   manage.py warmdb snapshot drop <label> [--cascade]
   manage.py warmdb snapshot prune [--keep N]
   manage.py warmdb snapshot status
   ```

5. **`exceptions.py`** — Add:
   - `WarmDBSnapshotNotFound`
   - `WarmDBSnapshotHasClones`

### Postgres Considerations

- `CREATE DATABASE ... TEMPLATE` requires that **no other sessions** are connected
  to the template database. Both `snapshot create` (source DB) and
  `snapshot clone` (snapshot DB) need this.
- `terminate_sessions()` already exists and handles this.
- For `snapshot create`, warn the user to stop their dev server first.
  If the terminate fails or new connections race in, the CREATE will fail
  with a clear Postgres error — no special handling needed.
- Snapshot databases should be left idle (no connections) so that cloning
  from them is always fast and doesn't require terminating anything.

### Testing

- Unit tests for naming functions and state table operations.
- Integration tests (gated behind `WARMDB_INTEGRATION=1`):
  - Create a snapshot from a test database.
  - Clone from a snapshot.
  - Drop clone, drop snapshot.
  - Prune with --keep.
  - Verify clone has the same data as the source.
  - Error cases: clone from nonexistent snapshot, drop snapshot with active clones.

## Implementation Order

1. `naming.py` — add snapshot naming functions
2. `state.py` — add snapshot/clone tables and methods
3. `snapshot.py` — core snapshot logic
4. `management/commands/warmdb.py` — wire up subcommands
5. `exceptions.py` — add new exception classes
6. Tests — unit + integration
7. Update AGENTS.md/CLAUDE.md with new commands

## Integration with worktree-setup.sh (in str-prod repo)

After the warmdb snapshot feature is implemented, `worktree-setup.sh --isolated`
will be updated to:

1. Check if any snapshots exist (`manage.py warmdb snapshot list`).
   If none, warn and skip DB cloning (or error with instructions).
2. Clone a DB: `manage.py warmdb snapshot clone <worktree-name>`.
3. Set `DBNAME=<clone_db_name>` in the worktree's `.env`.
4. On worktree teardown (future: a `worktree-teardown.sh`), call
   `manage.py warmdb snapshot drop-clone <worktree-name>`.

The worktree-setup integration is in the str-prod repo, not here. This plan
covers only the warmdb side.
