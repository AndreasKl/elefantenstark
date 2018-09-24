CREATE TABLE IF NOT EXISTS queue (
  id bigserial PRIMARY KEY,

  created timestamp DEFAULT (NOW() at time zone 'utc') NOT NULL,
  updated timestamp DEFAULT (NOW() at time zone 'utc') NOT NULL,
  processed boolean DEFAULT FALSE NOT NULL,
  times_processed int DEFAULT 0 NOT NULL,

  "key" text,
  value text,
  version bigint,
  data_map bytea,

  hash int
);

CREATE INDEX IF NOT EXISTS queue_id_where_not_processed_idx ON queue (id) WHERE NOT processed;
