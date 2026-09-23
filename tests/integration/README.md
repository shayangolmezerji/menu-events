# Integration tier

These tests run the SQL in `store/postgres.py` and `projections/postgres.py`
against a real server, and are skipped while `MENU_EVENTS_TEST_DSN` is unset, so
a green run without one proves nothing about either file.

Create a scratch database, apply both migrations to it, export the DSN, and run
the tier:

```bash
createdb menu_events_scratch
psql menu_events_scratch -f migrations/0001_event_store.sql
psql menu_events_scratch -f migrations/0002_projection.sql
export MENU_EVENTS_TEST_DSN="host=localhost dbname=menu_events_scratch user=postgres"
pytest tests/integration
```

Run it from the repository root so `pyproject.toml` supplies the marker
registration and the import path. The log is append-only by design, so nothing
here cleans up after itself: point the DSN at a database you are willing to
drop, never at one holding data you want.

`.env.example` at the repository root is the template for that variable. Nothing
in the package reads the file, so the value has to reach the process environment
before `pytest` starts.
