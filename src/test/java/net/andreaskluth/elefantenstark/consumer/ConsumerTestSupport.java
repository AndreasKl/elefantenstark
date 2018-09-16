package net.andreaskluth.elefantenstark.consumer;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.WorkItem;
import net.andreaskluth.elefantenstark.consumer.Consumer.WorkItemContext;

class ConsumerTestSupport {

  static void assertNextWorkItemIsCaptured(WorkItem expected, WorkItemContext actual) {
    assertAll("work", () -> assertEquals(expected, actual.workItem()));
  }

  static Optional<Object> capturingConsume(
      Connection connection, Consumer consumer, AtomicReference<WorkItemContext> capture) {
    return consumer.next(
        connection,
        wic -> {
          capture.set(wic);
          return new Object();
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
