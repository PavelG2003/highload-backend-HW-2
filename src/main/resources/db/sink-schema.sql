CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID NOT NULL,
    user_id TEXT NOT NULL,
    seq BIGINT NOT NULL CHECK (seq > 0),
    consumer_id TEXT NOT NULL,
    source_topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS processed_events_event_id_idx ON processed_events (event_id);

ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS write_order BIGINT GENERATED ALWAYS AS IDENTITY;

CREATE TABLE IF NOT EXISTS sink_counters (
    counter_name TEXT PRIMARY KEY,
    counter_value BIGINT NOT NULL
);
