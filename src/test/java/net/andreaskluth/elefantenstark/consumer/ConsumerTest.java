package net.andreaskluth.elefantenstark.consumer;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresAndSchema;
import static net.andreaskluth.elefantenstark.TestData.scheduleThreeWorkItems;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.assertNextWorkItemIsCaptured;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.capturingConsume;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.failingConsume;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.WorkItem;
import org.junit.jupiter.api.Test;

class ConsumerTest {

  @Test
  void fetchesAndDistributesWorkOrderedByVersion() {
    fetchesAndDistributesWorkOrderedByVersion(Consumer.transactionScoped());
    fetchesAndDistributesWorkOrderedByVersion(Consumer.sessionScoped());
  }

  private void fetchesAndDistributesWorkOrderedByVersion(Consumer consumer) {
    AtomicReference<WorkItem> capturedWork = new AtomicReference<>();
    withPostgresAndSchema(
        connection -> {
          scheduleThreeWorkItems(connection);
          capturingConsume(connection, consumer, capturedWork);
        });
    assertNextWorkItemIsCaptured(capturedWork.get(), new WorkItem("a", "b", 23));
  }

  @Test
  void whenTheWorkerFailsTheWorkCanBeReConsumed() {
    whenTheWorkerFailsTheWorkCanBeReConsumed(Consumer.transactionScoped());
    whenTheWorkerFailsTheWorkCanBeReConsumed(Consumer.sessionScoped());
  }

  private void whenTheWorkerFailsTheWorkCanBeReConsumed(Consumer consumer) {
    AtomicReference<WorkItem> capturedWorkA = new AtomicReference<>();
    AtomicReference<WorkItem> capturedWorkB = new AtomicReference<>();

    withPostgresAndSchema(
        connection -> {
          scheduleThreeWorkItems(connection);
          failingConsume(connection, consumer);
          capturingConsume(connection, consumer, capturedWorkA);
          failingConsume(connection, consumer);
          failingConsume(connection, consumer);
          capturingConsume(connection, consumer, capturedWorkB);
        });

    assertNextWorkItemIsCaptured(capturedWorkA.get(), new WorkItem("a", "b", 23));
    assertNextWorkItemIsCaptured(capturedWorkB.get(), new WorkItem("a", "b", 24));
  }

  @Test
  void ifThereIsNoWorkNothingIsConsumed() {
    ifThereIsNoWorkNothingIsConsumed(Consumer.transactionScoped());
    ifThereIsNoWorkNothingIsConsumed(Consumer.sessionScoped());
  }

  private void ifThereIsNoWorkNothingIsConsumed(Consumer consumer) {
    AtomicReference<WorkItem> capturedWork = new AtomicReference<>();
    withPostgresAndSchema(connection -> capturingConsume(connection, consumer, capturedWork));
    assertNull(capturedWork.get());
  }
}
