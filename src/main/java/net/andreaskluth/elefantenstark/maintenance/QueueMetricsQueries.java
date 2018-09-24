package net.andreaskluth.elefantenstark.maintenance;

public interface QueueMetricsQueries {

  String COUNT = "SELECT COUNT(*) FROM queue";
  String COUNT_NOT_PROCESSED = "SELECT COUNT(*) FROM queue WHERE NOT processed";
  String COUNT_PROCESSED = "SELECT COUNT(*) FROM queue WHERE processed ";
  String COUNT_QUEUE_ADVISORY_LOCK =
      "SELECT COUNT(*) FROM pg_locks "
          + " WHERE locktype = 'advisory' "
          + "   AND classid = 'queue'::regclass::int;";
}
