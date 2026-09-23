# Integration tier

These tests run the SQL in `store/postgres.py` and `projections/postgres.py`
against a real server, and are skipped while `MENU_EVENTS_TEST_DSN` is unset, so
a green run without one proves nothing about either file.

## Serving one

The development machine has no `initdb`, `psql` or `createdb`, so the server and
its client both come from a container. It answers loopback only and holds nothing
but rows this tier writes, which is the entire reason `trust` is acceptable here
and nowhere else.

```bash
docker run -d --name menu-events-pg \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -e POSTGRES_DB=menu_events_scratch \
    -p 127.0.0.1:5544:5432 postgres:16-alpine
docker exec menu-events-pg pg_isready -U postgres -d menu_events_scratch
```

Apply both migrations in order through the container's own `psql`, as the role the
DSN will connect with: `0001` creates `menu_app` and grants against it, and it is
the append-only trigger, the half that file calls load-bearing, that has to fire
for whoever runs the tier.

```bash
docker exec -i menu-events-pg psql -U postgres -d menu_events_scratch \
    -v ON_ERROR_STOP=1 -f - < migrations/0001_event_store.sql
docker exec -i menu-events-pg psql -U postgres -d menu_events_scratch \
    -v ON_ERROR_STOP=1 -f - < migrations/0002_projection.sql
```

Then point the variable at the published port. The value reaches
`psycopg.connect` unchanged, so a DSN that would prompt for a password stalls the
tier in the middle of a run:

```bash
export MENU_EVENTS_TEST_DSN="host=127.0.0.1 port=5544 dbname=menu_events_scratch user=postgres"
pytest tests/integration
```

Nothing is kept, so nothing is reused between runs. With no volume behind it, the
container is the database, and discarding it is the cleanup:

```bash
docker rm -f menu-events-pg
```

## Reading it

Run it from the repository root so `pyproject.toml` supplies the marker
registration and the import path. The log is append-only by design, so nothing
here cleans up after itself: point the DSN at a database you are willing to
drop, never at one holding data you want. Each tier is its own `pytest`
invocation, which is also how `.github/workflows/ci.yml` runs them, because the
test modules do `from conftest import ...` and a combined path like
`pytest tests/unit tests/api` can bind that name to only one of the files.

`.env.example` at the repository root is the template for that variable. Nothing
in the package reads the file, so the value has to reach the process environment
before `pytest` starts.

The tier was last run on 2026-09-24, against PostgreSQL 16.15 in a throwaway
container: 18 tests, all passing. That run published the port
`.github/workflows/ci.yml` names and applied both migrations through the
repository's own `psycopg` rather than the container's `psql`, which is the other
way the file above says to do it. Two of the 18 race two projection writers
against each other, so they are the ones to re-run if anything in
`projections/postgres.py` changes. Three race two writers on the log, and those
are the ones to re-run if the advisory lock or the version check in
`store/postgres.py` changes.
