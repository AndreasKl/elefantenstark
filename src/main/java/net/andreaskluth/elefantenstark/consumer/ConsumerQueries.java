package net.andreaskluth.elefantenstark.consumer;

class ConsumerQueries {

  static String SIMPLE_OBTAIN_WORK_QUERY_TRANSACTION_SCOPED =
      "DELETE FROM queue "
          + "WHERE id = ( "
          + "  SELECT id "
          + "  FROM queue "
          + "  ORDER BY id"
          + "  FOR UPDATE SKIP LOCKED "
          + "  LIMIT 1 "
          + ")"
          + "RETURNING *;";

  static String OBTAIN_WORK_QUERY_TRANSACTION_SCOPED =
      "UPDATE"
          + " queue"
          + " SET available = FALSE"
          + " WHERE id ="
          + "    ("
          + "        SELECT id FROM "
          + "        ("
          + "            SELECT"
          + "                id, \"key\""
          + "            FROM queue"
          + "            WHERE available"
          + "            ORDER BY id"
          + "            LIMIT 16"
          + "        ) ss"
          + "        WHERE pg_try_advisory_xact_lock('queue'::regclass::int, ('x'||substr(md5(\"key\"),1,8))::bit(32)::int)"
          + "        ORDER BY id"
          + "        LIMIT 1"
          + "    )"
          + " AND available"
          + " RETURNING *;";

  static String OBTAIN_WORK_QUERY_SESSION_SCOPED =
      "UPDATE"
          + " queue"
          + " SET available = FALSE"
          + " WHERE id ="
          + "    ("
          + "        SELECT id FROM "
          + "        ("
          + "            SELECT"
          + "                id, \"key\""
          + "            FROM queue"
          + "            WHERE available"
          + "            ORDER BY id"
          + "            LIMIT 16"
          + "        ) ss"
          + "        WHERE pg_try_advisory_lock('queue'::regclass::int, ('x'||substr(md5(\"key\"),1,8))::bit(32)::int)"
          + "        ORDER BY id"
          + "        LIMIT 1"
          + "    )"
          + " AND available"
          + " RETURNING *;";

  static final String UNLOCK_WORK_QUERY_SESSION_SCOPED =
      "SELECT pg_advisory_unlock('queue'::regclass::int, ('x'||substr(md5(?),1,8))::bit(32)::int);";

  private ConsumerQueries() {
    throw new UnsupportedOperationException("Not permitted");
  }
}
