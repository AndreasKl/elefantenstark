package net.andreaskluth.elefantenstark.consumer;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresAndSchema;
import static net.andreaskluth.elefantenstark.TestData.scheduleThreeWorkItems;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.assertNextWorkItemIsCaptured;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.capturingConsume;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.failingConsume;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.consumer.Consumer.WorkItemContext;
import net.andreaskluth.elefantenstark.work.WorkItem;
import org.junit.jupiter.api.Test;

class ConsumerTest {

  @Test
  void fetchesAndDistributesWorkOrderedByVersion() {
    fetchesAndDistributesWorkOrderedByVersion(Consumer.transactionScoped());
    fetchesAndDistributesWorkOrderedByVersion(Consumer.sessionScoped());
  }

  private void fetchesAndDistributesWorkOrderedByVersion(Consumer consumer) {
    AtomicReference<WorkItemContext> capturedWork = new AtomicReference<>();
    withPostgresAndSchema(
        connection -> {
          scheduleThreeWorkItems(connection);
          capturingConsume(connection, consumer, capturedWork);
        });
    assertNextWorkItemIsCaptured(WorkItem.hashedOnKey("a", "b", 23), capturedWork.get());
  }

  @Test
  void whenTheWorkerFailsTheWorkCanBeReConsumed() {
    whenTheWorkerFailsTheWorkCanBeReConsumed(Consumer.transactionScoped());
    whenTheWorkerFailsTheWorkCanBeReConsumed(Consumer.sessionScoped());
  }

  private void whenTheWorkerFailsTheWorkCanBeReConsumed(Consumer consumer) {
    AtomicReference<WorkItemContext> captureA = new AtomicReference<>();
    AtomicReference<WorkItemContext> captureB = new AtomicReference<>();
    AtomicReference<WorkItemContext> captureC = new AtomicReference<>();

    withPostgresAndSchema(
        connection -> {
          scheduleThreeWorkItems(connection);
          failingConsume(connection, consumer);
          capturingConsume(connection, consumer, captureA);
          failingConsume(connection, consumer);
          failingConsume(connection, consumer);
          capturingConsume(connection, consumer, captureB);
          capturingConsume(connection, consumer, captureC);
        });

    assertEquals(consumer.supportsStatefulProcessing() ? 2 : 0, captureA.get().timesProcessed());
    assertNextWorkItemIsCaptured(WorkItem.hashedOnKey("a", "b", 23), captureA.get());
    assertEquals(consumer.supportsStatefulProcessing() ? 3 : 0, captureB.get().timesProcessed());
    assertNextWorkItemIsCaptured(WorkItem.hashedOnKey("a", "b", 24), captureB.get());
    assertEquals(consumer.supportsStatefulProcessing() ? 1 : 0, captureC.get().timesProcessed());
    assertNextWorkItemIsCaptured(WorkItem.hashedOnKey("c", "d", 12), captureC.get());
  }

  @Test
  void ifThereIsNoWorkNothingIsConsumed() {
    ifThereIsNoWorkNothingIsConsumed(Consumer.transactionScoped());
    ifThereIsNoWorkNothingIsConsumed(Consumer.sessionScoped());
  }

  private void ifThereIsNoWorkNothingIsConsumed(Consumer consumer) {
    AtomicReference<WorkItemContext> capturedWork = new AtomicReference<>();
    withPostgresAndSchema(
        connection -> {
          Optional<Object> workResult = capturingConsume(connection, consumer, capturedWork);
          assertFalse(workResult.isPresent());
        });
    assertNull(capturedWork.get());
  }
}
