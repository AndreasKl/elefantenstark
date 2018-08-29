package net.andreaskluth.elefantenstark.consumer;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresAndSchema;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.WorkItem;
import net.andreaskluth.elefantenstark.producer.Producer;
import org.junit.jupiter.api.Test;

class ConsumerTest {

  @Test
  void fetchesAndDistributesWorkOrderedByVersion() throws Exception {
    AtomicReference<WorkItem> capturedWork = new AtomicReference<>();

    withPostgresAndSchema(
        connection -> {
          scheduleSomeWork(connection);
          capturingConsume(capturedWork, connection);
        });

    assertNextWorkItemIsCaptured(capturedWork.get(), new WorkItem("a", "b", 23));
  }

  @Test
  void whenTheWorkerFailsTheWorkCanBeReConsumed() throws Exception {
    AtomicReference<WorkItem> capturedWorkA = new AtomicReference<>();
    AtomicReference<WorkItem> capturedWorkB = new AtomicReference<>();

    withPostgresAndSchema(
        connection -> {
          scheduleSomeWork(connection);
          failingConsume(connection);
          capturingConsume(capturedWorkA, connection);
          failingConsume(connection);
          capturingConsume(capturedWorkB, connection);
        });

    assertNextWorkItemIsCaptured(capturedWorkA.get(), new WorkItem("a", "b", 23));
    assertNextWorkItemIsCaptured(capturedWorkB.get(), new WorkItem("c", "d", 12));
  }

  @Test
  void ifThereIsNoWorkNothingIsConsumed() throws Exception {
    AtomicReference<WorkItem> capturedWork = new AtomicReference<>();

    withPostgresAndSchema(connection -> capturingConsume(capturedWork, connection));

    assertNull(capturedWork.get());
  }

  private void capturingConsume(AtomicReference<WorkItem> work, Connection connection) {
    new Consumer().next(work::set).accept(connection);
  }

  private void failingConsume(Connection connection) {
    try {
      new Consumer()
          .next(
              workItem -> {
                throw new IllegalStateException();
              })
          .accept(connection);
    } catch (RuntimeException ex) {
      // Ignore
    }
  }

  private void assertNextWorkItemIsCaptured(WorkItem actual, WorkItem expected) {
    assertAll("work", () -> assertEquals(expected, actual));
  }

  private void scheduleSomeWork(Connection connection) {
    Producer producer = new Producer();
    producer.produce(new WorkItem("a", "b", 23)).apply(connection);
    producer.produce(new WorkItem("c", "d", 12)).apply(connection);
    producer.produce(new WorkItem("a", "b", 24)).apply(connection);
  }
}
