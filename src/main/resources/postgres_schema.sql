CREATE TABLE IF NOT EXISTS queue (
  id bigserial PRIMARY KEY,
  available boolean DEFAULT TRUE NOT NULL,
  times_processed int DEFAULT 0 NOT NULL,
  "key" text,
  value text,
  "group" int,
  data_map bytea,
  version bigint
);

CREATE INDEX IF NOT EXISTS queue_id_where_available_idx ON queue (id) WHERE available;
