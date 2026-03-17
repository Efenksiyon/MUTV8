-- Run this in the Supabase SQL Editor:
-- Dashboard -> SQL Editor -> New Query -> Paste & Run

CREATE TABLE IF NOT EXISTS market_history (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    market_id   TEXT             NOT NULL,
    market_question TEXT         NOT NULL,
    category    TEXT,
    yes_price   DOUBLE PRECISION,
    no_price    DOUBLE PRECISION,
    volume      DOUBLE PRECISION
);

-- Index for fast lookups by market and time
CREATE INDEX IF NOT EXISTS idx_market_history_market_id  ON market_history (market_id);
CREATE INDEX IF NOT EXISTS idx_market_history_timestamp  ON market_history (timestamp DESC);
