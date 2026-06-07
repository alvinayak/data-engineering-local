CREATE TABLE IF NOT EXISTS ohlc_results (
    symbol              VARCHAR(10)     NOT NULL,
    window_start        TIMESTAMP       NOT NULL,
    window_end          TIMESTAMP       NOT NULL,
    open                DECIMAL(12,2),
    high                DECIMAL(12,2),
    low                 DECIMAL(12,2),
    close               DECIMAL(12,2),
    event_count         INT,
    first_message_id    BIGINT,         -- Snowflake ID of first event in window
    last_message_id     BIGINT,         -- Snowflake ID of last event in window
    created_at          TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (symbol, window_start)
);

CREATE TABLE IF NOT EXISTS anomalies (
    id                  BIGINT          PRIMARY KEY,  -- Snowflake ID as PK
    symbol              VARCHAR(10)     NOT NULL,
    window_start        TIMESTAMP       NOT NULL,
    price_change_pct    DECIMAL(8,4),
    open                DECIMAL(12,2),
    close               DECIMAL(12,2),
    detected_at         TIMESTAMP       DEFAULT NOW()
);

-- Index for time-based queries — Snowflake IDs are naturally sortable
CREATE INDEX IF NOT EXISTS idx_ohlc_window_start ON ohlc_results(window_start);
CREATE INDEX IF NOT EXISTS idx_anomalies_symbol ON anomalies(symbol);
CREATE INDEX IF NOT EXISTS idx_anomalies_detected ON anomalies(detected_at);