package net.andreaskluth.elefantenstark.consumer;

public interface ConsumerQueries {

  String TRANSACTION_SCOPED_OBTAIN_WORK_QUERY =
      "UPDATE queue"
          + " SET"
          + "   processed = TRUE,"
          + "   updated = NOW()"
          + " WHERE id ="
          + "    ("
          + "        SELECT id FROM "
          + "        ("
          + "            SELECT"
          + "                id, hash"
          + "            FROM queue"
          + "            WHERE NOT processed"
          + "            ORDER BY id"
          + "            LIMIT 64"
          + "        ) akh"
          + "        WHERE pg_try_advisory_xact_lock('queue'::regclass::int, hash)"
          + "        ORDER BY id"
          + "        LIMIT 1"
          + "    )"
          + " AND NOT processed"
          + " RETURNING *;";

  String SESSION_SCOPED_OBTAIN_WORK_QUERY =
      "UPDATE queue"
          + " SET"
          + "   times_processed = times_processed + 1,"
          + "   updated = NOW()"
          + " WHERE id ="
          + "    ("
          + "        SELECT id FROM "
          + "        ("
          + "            SELECT"
          + "                id, hash"
          + "            FROM queue"
          + "            WHERE NOT processed"
          + "            ORDER BY id"
          + "            LIMIT 64"
          + "        ) akh"
          + "        WHERE pg_try_advisory_lock('queue'::regclass::int, hash)"
          + "        ORDER BY id"
          + "        LIMIT 1"
          + "    )"
          + " AND NOT processed"
          + " RETURNING *;";

  String SESSION_SCOPED_MARK_AS_PROCESSED = "UPDATE queue SET processed = TRUE WHERE id = ?";

  String SESSION_SCOPED_UNLOCK_ADVISORY_LOCK =
      "SELECT pg_advisory_unlock('queue'::regclass::int, ?);";
}
