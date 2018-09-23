package net.andreaskluth.elefantenstark.maintenance;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresConnectionsAndSchema;
import static net.andreaskluth.elefantenstark.TestData.scheduleThreeWorkItems;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import net.andreaskluth.elefantenstark.consumer.Consumer;
import org.junit.jupiter.api.Test;

class QueueMetricsTest {

  @Test
  void validateQueueMonitoring() {
    withPostgresConnectionsAndSchema(
        connections -> {
          Connection connection = connections.get();

          QueueMetrics metrics = QueueMetrics.metrics();

          long initialSize = metrics.size(connection);

          scheduleThreeWorkItems(connection);

          long size = metrics.size(connection);
          long sizeNotProcessed = metrics.sizeNotProcessed(connection);
          long sizeProcessed = metrics.sizeProcessed(connection);

          assertAll(
              "metrics",
              () -> {
                assertEquals(0, initialSize);
                assertEquals(3, size);
                assertEquals(3, sizeNotProcessed);
                assertEquals(0, sizeProcessed);
              });
        });
  }

  @Test
  void validateLockMonitoring() {
    withPostgresConnectionsAndSchema(
        connections -> {
          Connection connection = connections.get();

          createAnArbitraryLock(connection);

          QueueMetrics metrics = QueueMetrics.metrics();

          long initialLocks = metrics.currentLocks(connection);

          scheduleThreeWorkItems(connection);

          Consumer consumer = Consumer.sessionScoped();
          long locks =
              consumer.next(connection, workItem -> metrics.currentLocks(connection)).orElse(0L);

          assertAll(
              "metrics",
              () -> {
                assertEquals(0, initialLocks);
                assertEquals(1, locks);
              });
        });
  }

  private void createAnArbitraryLock(Connection connection) {
    try {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("SELECT pg_advisory_lock(22);")) {
        preparedStatement.execute();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
