CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    quantity INTEGER NOT NULL,
    category TEXT NOT NULL,
    location TEXT NOT NULL,
    device TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    algorithm TEXT NOT NULL DEFAULT 'heuristic',
    risk_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    risk_level TEXT NOT NULL DEFAULT 'LOW',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS events_timestamp_idx ON events (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS events_risk_idx ON events (risk_level);
ALTER TABLE events ADD COLUMN IF NOT EXISTS algorithm TEXT NOT NULL DEFAULT 'heuristic';
