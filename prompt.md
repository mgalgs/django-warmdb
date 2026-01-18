### I'd like to have pre-cached postgres DBs ready for running django tests (migrations already applied)

So that I don't have to wait around for db creation and migrations. On test runs, we
would allocate one of the available pre-cached dbs and run the tests with
`--keepdb`. That way every test run gets a fresh DB but doesn't have to wait for
migrations.

We should pre-cache 5 by default, make that configurable. Should be under the `init`
subcommand, which allocates the DBs and applies migrations in them. Save state in a
local sqlite db (not via django, just the standard library) at the project root.

A `status` subcommand can be run at any time and shows all of the dbs and their
status ("initializing", "ready", "in-use", "done").

`invalidate` drops all dbs.

Can we automatically detect when the db schema changes and flag the user? Maybe
during test startup if we detect that there are unapplied migrations (can we do
that?) we output the command necessary to re-sync pre-warmed DBs (`$CMD flush && $CMD
init`) and fail the tests.

Can this be a management command? Perhaps a dedicated app? Or does it need to be
implemented as part of my app's test scripts?


Yes—this can be done cleanly as a **dedicated Django app** that provides:

1) a **management command** (`manage.py warmdb init|status|invalidate`)
2) a **custom test runner** (so allocation + schema checks happen automatically at test startup)

A management command alone can’t “inject” an already-created DB into `manage.py test` reliably; you need a test runner (or an external wrapper script) to swap the database name before Django starts its normal test DB creation/migration flow.

---

## The fast approach: Postgres template DB + pooled clones

Instead of “5 pre-migrated DBs that tests dirty”, the fastest and cleanest loop is:

- Create **one migrated template database** (schema is correct, empty)
- Maintain a pool of N databases that are **cloned from the template** using Postgres:

```sql
CREATE DATABASE warmtest_03 TEMPLATE warmtemplate;
```

Cloning from a template is typically *much* faster than running migrations.

### Lifecycle
- `warmdb init`: (re)build template DB (run migrations once), then create/refresh N clones from it → mark `ready`
- test startup: allocate a `ready` clone → mark `in-use`, point Django test connection at it
- test end: drop & recreate that clone from template (fast) → mark `ready` again
  (you can still show a transient `done` state if you want, but practically you’ll want to recycle immediately so the pool stays full)

---

## State tracking in local sqlite (stdlib)

At project root: `warmdb_state.sqlite3`

Suggested schema:

- `meta(key TEXT PRIMARY KEY, value TEXT)` (store `schema_hash`, `template_name`, pool size, etc.)
- `dbs(name TEXT PRIMARY KEY, status TEXT, allocated_to_pid INT, allocated_at TEXT, last_error TEXT, schema_hash TEXT)`

Allocation must be atomic (concurrency-safe):
- `BEGIN IMMEDIATE;`
- pick one `ready`
- update it to `in-use` with pid/timestamp
- `COMMIT;`

Also handle stale `in-use` entries:
- if pid no longer exists (or older than TTL), reclaim by resetting it.

---

## Subcommands

### `manage.py warmdb init [--pool-size 5]`
- compute `schema_hash` (more below)
- drop old pool/template if hash differs (or if `--force`)
- create template DB
- run `migrate` against template DB
- create/refresh N clone DBs from template
- write sqlite state: all `ready`

### `manage.py warmdb status`
Show each DB and status:
- initializing / ready / in-use / done (plus pid, age, schema_hash)

### `manage.py warmdb invalidate`
- drop all warmdb databases (template + clones)
- delete sqlite state (or mark invalid)

(“flush” in Django terms isn’t what you want here; you want drop/recreate-from-template.)

---

## Detecting schema changes (and failing tests)

You can do this two ways; I’d do both:

### 1) Fast: migration-files fingerprint
Compute a `schema_hash` from:
- all `*/migrations/*.py` contents (or mtimes + sizes + filenames)
- Django version + installed apps list (optional but helpful)

Store that hash in sqlite during `init`.

At test startup:
- recompute hash
- if mismatch: fail immediately with instruction:

```
manage.py warmdb invalidate && manage.py warmdb init
```

This catches “migration file changed” instantly without touching Postgres.

### 2) Definitive: check for unapplied migrations on allocated DB
After allocating a DB (connecting to it), run:

- `MigrationExecutor(connection).migration_plan(targets)`
- if plan is non-empty ⇒ unapplied migrations exist ⇒ fail tests with the same instruction

Yes, Django can detect unapplied migrations programmatically; it’s essentially what `showmigrations --plan` uses.

---

## How to hook into `manage.py test`

### Best: custom test runner
Implement e.g. `warmdb/runner.py`:

- allocate a DB from sqlite pool
- override the connection settings to point at that DB name
- **bypass Django’s create_test_db/migrate** steps (otherwise Django will still do work)
- on teardown: recycle DB (drop + recreate from template) and mark `ready`

You do this by subclassing `DiscoverRunner` and overriding `setup_databases()` / `teardown_databases()`.

Configure:

```python
TEST_RUNNER = "warmdb.runner.WarmDBDiscoverRunner"
```

This keeps the UX as:

```bash
manage.py test
```

No wrapper script required.

### Notes
- If you use `--parallel`, Django normally wants multiple databases (one per worker). You can support that by allocating **k DBs** per run (k = workers) or by disabling `--parallel` for now.
- Your Postgres user must have `CREATEDB` (and privileges to drop those DBs).

---

## Packaging recommendation

Create a small dedicated app, e.g. `warmdb/`:

- `management/commands/warmdb.py` (init/status/invalidate)
- `pool.py` (sqlite state, allocate/release, schema hashing)
- `postgres.py` (create/drop db, clone-from-template using `_nodb_cursor()` or psycopg)
- `runner.py` (custom DiscoverRunner)

This keeps it versionable, testable, and shareable across projects, and avoids burying it in ad-hoc shell scripts.

---

If you tell me:
- Django version
- whether you use `pytest-django` or vanilla `manage.py test`
- whether you need `--parallel`

…I can sketch concrete code for the runner override points and the Postgres template cloning bits (using Django’s `_nodb_cursor()` so you don’t have to shell out to `psql`).

### I use `manage.py test`, don't need `--parallel`
