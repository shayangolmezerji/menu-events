-- 0001: the event log.
--
-- One table, and the only table in this schema whose rows cannot be rewritten.
-- Everything in 0002 is derived from it and can be dropped and recomputed.
--
-- Append-only is enforced from two directions on purpose:
--
--   * A statement trigger refusing UPDATE, DELETE and TRUNCATE. Triggers fire
--     for the table owner and for a superuser, which no grant can prevent, so
--     this is the load-bearing half.
--   * Grants: the application role gets INSERT and SELECT and nothing else.
--     That covers the ordinary path, and turns a mistake by an operator into a
--     permission error instead of a silently edited row.
--
-- What neither of them does: a superuser can DROP TRIGGER, and anyone able to
-- do that can also reload the table from a file. The last line of defence is
-- the checksum column plus a backup you trust, not this file. It is written
-- down here because a guarantee with a condition nobody mentions is worse than
-- no guarantee at all.
--
-- Apply with:
--     psql "$DATABASE_URL" -f migrations/0001_event_store.sql

CREATE TABLE events (
    stream_id   text        NOT NULL,
    version     bigint      NOT NULL,

    -- Version is the stream position, starting at 1, and is what a command's
    -- expected_menu_version refers to. Zero is reserved for "no events yet",
    -- which is never a row.
    CHECK (version > 0),

    event_id    uuid        NOT NULL,
    command_id  uuid        NOT NULL,
    event_type  text        NOT NULL,
    payload     jsonb       NOT NULL,

    -- now() is the transaction start, not the statement start, so every event
    -- committed by one transaction shares a timestamp. Ordering never depends
    -- on it: the version does that work.
    recorded_at timestamptz NOT NULL DEFAULT now(),

    -- sha256 of the payload serialised with sorted keys, computed by the
    -- writer. jsonb reorders and normalises what it stores, so a checksum over
    -- the column text would not survive a round trip; this one is defined
    -- against the parsed payload and therefore does.
    checksum    text        NOT NULL,

    PRIMARY KEY (stream_id, version),

    -- Later events point at earlier ones through `corrects`, so an id has to be
    -- unique across every stream, not merely within one.
    UNIQUE (event_id),

    -- Idempotency key. One event per command, so a retry of a command that
    -- already landed is found by this index rather than by scanning the stream.
    UNIQUE (stream_id, command_id),

    -- The column and the document have to agree, otherwise a row can be read
    -- two different ways by two different consumers of the same log.
    CHECK (event_type = payload ->> 'event_type'),
    CHECK (checksum ~ '^[0-9a-f]{64}$')
);

CREATE FUNCTION reject_event_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'the events table is append-only: % is not permitted', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE TRIGGER events_are_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON events
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_event_mutation();

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'menu_app') THEN
        CREATE ROLE menu_app NOLOGIN;
    END IF;
END;
$$;

-- The role an application connects as, named by the DSN you deploy with. The test
-- tier reads MENU_EVENTS_TEST_DSN instead, and .env.example is its template.
GRANT SELECT, INSERT ON events TO menu_app;
REVOKE UPDATE, DELETE, TRUNCATE ON events FROM menu_app;
REVOKE UPDATE, DELETE, TRUNCATE ON events FROM PUBLIC;

COMMENT ON TABLE events IS
    'Append-only menu event log. UPDATE, DELETE and TRUNCATE are refused by trigger.';
COMMENT ON COLUMN events.version IS 'Stream position, 1-based. The menu_version in a command.';
COMMENT ON COLUMN events.command_id IS 'Idempotency key supplied by the writer, one event per command.';
COMMENT ON COLUMN events.checksum IS 'sha256 of the canonical payload JSON, written by the producer.';
