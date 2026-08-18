-- VidPG P3 schema: a logged control row and three disposable payload buckets.

CREATE SCHEMA IF NOT EXISTS vidpg;

CREATE TABLE IF NOT EXISTS vidpg.bucket_state (
    singleton   boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    generation  bigint NOT NULL CHECK (generation >= 0),
    active      smallint NOT NULL CHECK (active BETWEEN 0 AND 2),
    switched_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO vidpg.bucket_state (singleton, generation, active)
VALUES (true, 0, 0)
ON CONFLICT (singleton) DO NOTHING;

CREATE UNLOGGED TABLE IF NOT EXISTS vidpg.frame_bucket_0 (
    stream_id          uuid NOT NULL,
    seq                bigint NOT NULL CHECK (seq > 0),
    captured_us        bigint NOT NULL,
    relay_received_at  timestamptz NOT NULL,
    inserted_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    codec              smallint NOT NULL,
    width              smallint NOT NULL CHECK (width > 0),
    height             smallint NOT NULL CHECK (height > 0),
    frame              bytea STORAGE EXTERNAL NOT NULL,
    CHECK (octet_length(frame) BETWEEN 1 AND 1048576)
);

CREATE UNLOGGED TABLE IF NOT EXISTS vidpg.frame_bucket_1
    (LIKE vidpg.frame_bucket_0 INCLUDING ALL);

CREATE UNLOGGED TABLE IF NOT EXISTS vidpg.frame_bucket_2
    (LIKE vidpg.frame_bucket_0 INCLUDING ALL);

CREATE INDEX IF NOT EXISTS frame_bucket_0_latest
    ON vidpg.frame_bucket_0 (stream_id, seq DESC);

CREATE INDEX IF NOT EXISTS frame_bucket_1_latest
    ON vidpg.frame_bucket_1 (stream_id, seq DESC);

CREATE INDEX IF NOT EXISTS frame_bucket_2_latest
    ON vidpg.frame_bucket_2 (stream_id, seq DESC);
