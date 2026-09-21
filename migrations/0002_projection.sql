-- 0002: the menu projection, rebuilt by replaying events.
--
-- These tables are mutable, and that is the point: they hold derived state, so
-- a row that looks wrong is evidence of a projector bug and is fixed by
-- rebuilding from the log, never by updating it in place. Nothing here is a
-- source of truth, which is why there is no trigger and no checksum column.
--
-- The read model is the shape a menu endpoint wants: one row per item, current
-- values, no folding at request time. Its price can disagree with the log for
-- as long as the projector has not run, and that window is the cost of CQRS
-- being called eventual consistency rather than a bug.
--
-- Apply with:
--     psql "$DATABASE_URL" -f migrations/0002_projection.sql

-- How far the projector has gotten, per stream. Comparing version against the
-- log head on read is how a caller finds out whether it is being served a menu
-- that is behind.
CREATE TABLE menu_projection (
    stream_id   text        NOT NULL,
    version     bigint      NOT NULL DEFAULT 0,
    refreshed_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (stream_id),
    CHECK (version >= 0)
);

CREATE TABLE menu_item (
    stream_id    text        NOT NULL,
    item_id      uuid        NOT NULL,

    name         text        NOT NULL,
    price_cents  bigint      NOT NULL,
    description  text        NOT NULL DEFAULT '',
    category     text        NOT NULL DEFAULT 'menu',
    sold_out     boolean     NOT NULL DEFAULT false,

    -- Copied from the event that last touched this item, so a stale row is
    -- recognisable without joining back to the log.
    updated_at   timestamptz NOT NULL,
    updated_by   text        NOT NULL,

    PRIMARY KEY (stream_id, item_id),
    CHECK (price_cents >= 0),
    FOREIGN KEY (stream_id) REFERENCES menu_projection (stream_id) ON DELETE CASCADE
);

-- The menu card is read by category far more often than it is read by id.
CREATE INDEX menu_item_by_category ON menu_item (stream_id, category, name);

GRANT SELECT, INSERT, UPDATE, DELETE ON menu_projection, menu_item TO menu_app;

COMMENT ON TABLE menu_item IS
    'Derived state. Rebuilt from events by PostgresMenuReadModel; safe to truncate and replay.';
