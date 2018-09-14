package net.andreaskluth.elefantenstark.consumer;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.WorkItem;

class ConsumerTestSupport {

  static void assertNextWorkItemIsCaptured(WorkItem actual, WorkItem expected) {
    assertAll("work", () -> assertEquals(expected, actual));
  }

  static void capturingConsume(
      Connection connection, Consumer consumer, AtomicReference<WorkItem> capture) {
    consumer.next(
        connection,
        workItem -> {
          capture.set(workItem);
          return null;
        });
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
