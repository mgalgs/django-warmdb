# django-warmdb — pre-warmed Postgres DB pool for Django tests

## Goal
Speed up `manage.py test` by reusing a pool of **already-migrated** Postgres databases.

Instead of creating a test database and running migrations on every run, we:

1. Maintain one **template** database with migrations applied.
2. Maintain a pool of **N clone databases** created from the template via Postgres `CREATE DATABASE ... TEMPLATE ...`.
3. On test startup, allocate one ready clone and run tests against it (with `--keepdb`-style behavior).
4. On test teardown, recycle the clone (drop + re-clone from template) so the pool stays "fresh".

## Non-goals
- Supporting `--parallel` initially.
- Supporting DB backends other than Postgres.
- Replacing Django's test framework (this integrates with `manage.py test`).
- Supporting multiple database aliases initially (assume `DATABASES["default"]`).

## Delivery shape
Implement as a small **Django app** (e.g. `warmdb/`) providing:

- `manage.py warmdb init|status|invalidate`
- a custom test runner: `warmdb.runner.WarmDBDiscoverRunner`

Users opt in by adding to `settings.py`:

```py
INSTALLED_APPS += ["warmdb"]
TEST_RUNNER = "warmdb.runner.WarmDBDiscoverRunner"
```

## Requirements / assumptions
- Postgres user has permission to `CREATE DATABASE` and `DROP DATABASE`.
- Tests are run via `manage.py test` (no pytest runner integration required for v1).
- No `--parallel` support (runner should error clearly if requested).
- Django version: **>= 3.2** (so `DATABASES[alias]["TEST"]["MIGRATE"] = False` is available).

---

# CLI / management command

All state is stored locally in a sqlite database at the project root.

Best-practice: use `settings.BASE_DIR` (rather than the process CWD) so the state file location is stable regardless of where `manage.py` is invoked from.

- `state_path = Path(settings.BASE_DIR) / "warmdb_state.sqlite3"`

Command: `manage.py warmdb <subcommand> [options]`

## `warmdb init`
Prepares (or refreshes) the template DB and a pool of N clones.

Options:
- `--pool-size N` (default: 5)
- `--force` (drop/recreate even if schema hash matches)
- `--prefix warmdb` (optional; default `warmdb`)

Behavior:
1. Compute a `schema_hash` for the project (see "Schema change detection").
2. Ensure sqlite state exists.
3. If existing state's `schema_hash` differs from computed hash:
   - automatically rebuild everything (equivalent to `invalidate` + `init`) and print why.
4. If `--force` is provided, rebuild everything even if `schema_hash` matches.
5. Create (or recreate) the template DB.
6. Apply migrations to the template DB.
7. Create (or recreate) `pool_size` clone DBs from the template DB.
8. Mark all clones as `ready` in sqlite state.

Notes:
- Prefer Postgres template cloning (`CREATE DATABASE clone TEMPLATE template;`) because it is typically far faster than running migrations.
- Create/drop databases using Django's database backend machinery (e.g. `_nodb_cursor()`), not shelling out to `psql`.
- When dropping a database, first terminate existing sessions to avoid `DROP DATABASE` failing.

## `warmdb status`
Print pool status at any time.

Displays:
- template name
- stored `schema_hash`
- each clone DB name and status: `initializing | ready | in-use | error`
- (if in-use) pid + allocated_at age
- last_error (if any)

## `warmdb invalidate`
Drops all warm DBs (template + clones) and clears local state.

Behavior:
- Drop template DB and any clone DBs registered in sqlite.
- Remove `warmdb_state.sqlite3` (preferred) OR clear all rows.

---

# Runtime integration (test runner)

Implement `warmdb.runner.WarmDBDiscoverRunner`.

## Startup behavior
On `manage.py test`:

1. Ensure pool state exists.
   - If missing: fail fast with:

     ```
     warmdb is not initialized. Run: manage.py warmdb init
     ```

2. Schema hash check:
   - Compute current schema hash.
   - Compare with sqlite stored hash.
   - If mismatch: fail fast with:

     ```
     Schema changed since warmdb init.
     Run: manage.py warmdb invalidate && manage.py warmdb init
     ```

3. Reject `--parallel`:
   - If `parallel != 1`, raise a clear error stating warmdb does not support `--parallel` yet.

4. Allocate one `ready` DB from the pool (atomic allocation).
   - If none are available:
     - print current pool status
     - fail with a message suggesting to wait or increase pool size.

5. Point Django's test database name at the allocated DB.
   - Set `DATABASES[alias]["TEST"]["NAME"]` to the allocated DB name.
   - Set `DATABASES[alias]["TEST"]["MIGRATE"] = False`.
   - Force `keepdb=True` semantics (warmdb owns lifecycle of the clone).

6. Definitive migration check (recommended):
   - After connecting to the allocated DB, use `MigrationExecutor(connection)` and verify there is no unapplied migration plan.
   - If there are unapplied migrations, fail with the same "invalidate && init" message.

## Teardown behavior
On completion:

1. Recycle the allocated clone DB:
   - drop clone database
   - re-create clone from template database
2. Mark clone as `ready` again in sqlite.

If recycling fails:
- mark clone `error` with `last_error`
- avoid masking original test failures if possible (warn loudly on stderr and leave it `error`).

---

# State management (sqlite, stdlib)

State DB file: `warmdb_state.sqlite3` at project root.

## Tables

```sql
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dbs (
  name TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  allocated_to_pid INTEGER,
  allocated_at TEXT,
  last_error TEXT,
  schema_hash TEXT NOT NULL
);
```

Stored meta keys (suggested):
- `schema_hash`
- `template_db_name`
- `pool_size`
- `prefix`
- `created_at`

## Status values
- `initializing`
- `ready`
- `in-use`
- `error`

## Allocation (concurrency-safe)
Allocation must be atomic:

- `BEGIN IMMEDIATE;`
- select one row `WHERE status='ready'` (deterministic order)
- update to `in-use`, set pid and timestamp
- `COMMIT;`

Reclaiming stale locks:
- If a DB is `in-use` but pid no longer exists (best-effort `os.kill(pid, 0)`), it may be returned to `ready`.
- Optionally, enforce a TTL (e.g. 4h) for safety.

---

# Postgres database naming

Pick deterministic names derived from a prefix + schema hash so it's obvious when schema changes:

- template: `{prefix}_template_{hash8}`
- clones: `{prefix}_{hash8}_{i:02d}` for i in 1..N

Example:
- `warmdb_template_a1b2c3d4`
- `warmdb_a1b2c3d4_01` ... `warmdb_a1b2c3d4_05`

This makes it safe to keep old DBs around if a new init happens, and makes `invalidate` easy.

---

# Schema change detection

## Fast check (required)
Compute a `schema_hash` from:

- contents of all `*/migrations/*.py` files
  - include relative path + file bytes (bytes preferred for correctness)
- include Django version

Store `schema_hash` in sqlite during `warmdb init`.

At test startup, recompute and fail if mismatch.

## Definitive check (recommended)
After allocating DB, verify no unapplied migrations using Django's migration system (no shelling out):

- `MigrationExecutor(connection).migration_plan(targets)`
- if non-empty => fail tests with "invalidate && init" guidance

---

# UX / error messages

Failures should be actionable and include the exact command:

```
manage.py warmdb invalidate && manage.py warmdb init
```

If no DBs are available (`ready` empty), fail with:
- current pool status and hint to increase pool size or wait for running tests to finish.

---

# Acceptance criteria

- `manage.py warmdb init` creates a migrated template and N ready clones.
- `manage.py warmdb status` displays all DBs and their statuses.
- `manage.py warmdb invalidate` removes all warmdb databases and local state.
- `manage.py test`:
  - allocates a clone (marks `in-use`)
  - runs tests without creating/migrating a DB
  - on exit, recycles the clone and marks it `ready`
- If migrations change, `manage.py test` fails with clear instructions to re-init.
