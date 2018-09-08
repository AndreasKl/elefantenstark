CREATE TABLE IF NOT EXISTS queue (
  id bigserial PRIMARY KEY,
  available boolean DEFAULT TRUE NOT NULL,
  times_processed int DEFAULT 0 NOT NULL,
  "key" text,
  value text,
  version bigint
);

CREATE INDEX IF NOT EXISTS queue_key_version_idx ON queue ("key", version);
CREATE INDEX IF NOT EXISTS queue_key_where_available_idx ON queue ("key") WHERE available;
