package net.andreaskluth.elefantenstark.consumer;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresAndSchema;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.assertNextWorkItemIsCaptured;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.capturingConsume;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.failingConsume;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.scheduleSomeWork;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.WorkItem;
import org.junit.jupiter.api.Test;

class ConsumerTest {

  @Test
  void fetchesAndDistributesWorkOrderedByVersion() throws Exception {
    fetchesAndDistributesWorkOrderedByVersion(Consumer.transactionScoped());
    fetchesAndDistributesWorkOrderedByVersion(Consumer.sessionScoped());
  }

  private void fetchesAndDistributesWorkOrderedByVersion(Consumer consumer) throws IOException {
    AtomicReference<WorkItem> capturedWork = new AtomicReference<>();
    withPostgresAndSchema(
        connection -> {
          scheduleSomeWork(connection);
          capturingConsume(connection, consumer, capturedWork);
        });
    assertNextWorkItemIsCaptured(capturedWork.get(), new WorkItem("a", "b", 23));
  }

  @Test
  void whenTheWorkerFailsTheWorkCanBeReConsumed() throws Exception {
    whenTheWorkerFailsTheWorkCanBeReConsumed(Consumer.transactionScoped());
    whenTheWorkerFailsTheWorkCanBeReConsumed(Consumer.sessionScoped());
  }

  private void whenTheWorkerFailsTheWorkCanBeReConsumed(Consumer consumer) throws IOException {
    AtomicReference<WorkItem> capturedWorkA = new AtomicReference<>();
    AtomicReference<WorkItem> capturedWorkB = new AtomicReference<>();

    withPostgresAndSchema(
        connection -> {
          scheduleSomeWork(connection);
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
  void ifThereIsNoWorkNothingIsConsumed() throws Exception {
    ifThereIsNoWorkNothingIsConsumed(Consumer.transactionScoped());
    ifThereIsNoWorkNothingIsConsumed(Consumer.sessionScoped());
  }

  private void ifThereIsNoWorkNothingIsConsumed(Consumer consumer) throws IOException {
    AtomicReference<WorkItem> capturedWork = new AtomicReference<>();
    withPostgresAndSchema(connection -> capturingConsume(connection, consumer, capturedWork));
    assertNull(capturedWork.get());
  }
}
