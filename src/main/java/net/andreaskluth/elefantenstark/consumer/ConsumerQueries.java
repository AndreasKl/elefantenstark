package net.andreaskluth.elefantenstark.consumer;

public interface ConsumerQueries {

  String OBTAIN_WORK_QUERY_TRANSACTION_SCOPED =
      "UPDATE queue"
          + " SET"
          + "   available = FALSE"
          + " WHERE id ="
          + "    ("
          + "        SELECT id FROM "
          + "        ("
          + "            SELECT"
          + "                id, \"group\""
          + "            FROM queue"
          + "            WHERE available"
          + "            ORDER BY id"
          + "            LIMIT 64"
          + "        ) akh"
          + "        WHERE pg_try_advisory_xact_lock('queue'::regclass::int, \"group\")"
          + "        ORDER BY id"
          + "        LIMIT 1"
          + "    )"
          + " AND available"
          + " RETURNING *;";

  String OBTAIN_WORK_QUERY_SESSION_SCOPED =
      "UPDATE queue"
          + " SET"
          + "   times_processed = times_processed + 1"
          + " WHERE id ="
          + "    ("
          + "        SELECT id FROM "
          + "        ("
          + "            SELECT"
          + "                id, \"group\""
          + "            FROM queue"
          + "            WHERE available"
          + "            ORDER BY id"
          + "            LIMIT 64"
          + "        ) ss"
          + "        WHERE pg_try_advisory_lock('queue'::regclass::int, \"group\")"
          + "        ORDER BY id"
          + "        LIMIT 1"
          + "    )"
          + " AND available"
          + " RETURNING *;";
}
