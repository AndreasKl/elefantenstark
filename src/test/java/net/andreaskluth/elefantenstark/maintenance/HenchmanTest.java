package net.andreaskluth.elefantenstark.maintenance;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresConnectionsAndSchema;
import static net.andreaskluth.elefantenstark.TestData.scheduleThreeWorkItems;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.consumer.Consumer;
import net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport;
import org.junit.jupiter.api.Test;

class HenchmanTest {

  Henchman henchman = new Henchman();

  Consumer consumer = Consumer.transactionScoped();
  QueueMetrics queueMetrics = new QueueMetrics();

  @Test
  void deleteOldWorkItems() {
    withPostgresConnectionsAndSchema(
        connections -> {
          Connection connection = connections.get();

          scheduleThreeWorkItems(connection);
          consumeThreeWorkItems(connection);

          henchman.cleanupOldWorkItems(connection, Duration.ofHours(-1));

          assertEquals(0, queueMetrics.size(connection));
        });
  }

  private void consumeThreeWorkItems(Connection connection) {
    ConsumerTestSupport.capturingConsume(connection, consumer, new AtomicReference<>());
    ConsumerTestSupport.capturingConsume(connection, consumer, new AtomicReference<>());
    ConsumerTestSupport.capturingConsume(connection, consumer, new AtomicReference<>());
  }
}
