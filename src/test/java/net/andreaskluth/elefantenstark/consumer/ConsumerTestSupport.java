package net.andreaskluth.elefantenstark.consumer;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.WorkItem;
import net.andreaskluth.elefantenstark.producer.Producer;

class ConsumerTestSupport {

  static void assertNextWorkItemIsCaptured(WorkItem actual, WorkItem expected) {
    assertAll("work", () -> assertEquals(expected, actual));
  }

  static void scheduleSomeWork(Connection connection) {
    Producer producer = new Producer();
    producer.produce(connection, new WorkItem("a", "b", 23));
    producer.produce(connection, new WorkItem("a", "b", 24));
    producer.produce(connection, new WorkItem("c", "d", 12));
  }

  static void capturingConsume(
      Connection connection, Consumer consumer, AtomicReference<WorkItem> capture) {
    consumer.next(connection, capture::set);
  }

  static void failingConsume(Connection connection, Consumer consumer) {
    try {
      consumer.next(
          connection,
          workItem -> {
            throw new IllegalStateException();
          });
    } catch (IllegalStateException ignored) {
      // The exception should bubble up, ignore it here.
    }
  }

  private ConsumerTestSupport() {
    throw new UnsupportedOperationException("Not permitted");
  }
}
