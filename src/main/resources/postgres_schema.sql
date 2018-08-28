CREATE TABLE IF NOT EXISTS queue (
  id bigserial PRIMARY KEY,
  "key" text,
  value text,
  version bigint
);

CREATE INDEX IF NOT EXISTS queue_key_version_idx ON queue ("key", version);