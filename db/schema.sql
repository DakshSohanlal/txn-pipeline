CREATE TABLE IF NOT EXISTS transactions (
  event_id UUID PRIMARY KEY,
  user_id TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('card', 'transfer')),
  amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
  currency CHAR(3) NOT NULL,
  merchant_name TEXT,
  merchant_category TEXT,
  occurred_at TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_txn_user_time ON transactions (user_id, occurred_at);

CREATE TABLE IF NOT EXISTS daily_spend (
  user_id TEXT NOT NULL,
  currency CHAR(3) NOT NULL,
  merchant_category TEXT NOT NULL,
  day DATE NOT NULL,
  total_minor BIGINT NOT NULL,
  txn_count INT NOT NULL,
  PRIMARY KEY (user_id, currency, merchant_category, day)
);

CREATE TABLE IF NOT EXISTS anomalies (
  user_id TEXT NOT NULL,
  currency CHAR(3) NOT NULL,
  day DATE NOT NULL,
  daily_total_minor BIGINT NOT NULL,
  trailing_avg_minor BIGINT NOT NULL,
  ratio NUMERIC(10, 2) NOT NULL,
  PRIMARY KEY (user_id, currency, day)
);
-- GET /anomalies filters and sorts by day
CREATE INDEX IF NOT EXISTS idx_anomalies_day ON anomalies (day DESC);
