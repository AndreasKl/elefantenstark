package net.andreaskluth.elefantenstark.consumer;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresAndSchema;
import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresConnectionsAndSchema;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
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
    assertNextWorkItemIsCaptured(capturedWorkB.get(), new WorkItem("a", "b", 24));
  }

  @Test
  void ifThereIsNoWorkNothingIsConsumed() throws Exception {
    AtomicReference<WorkItem> capturedWork = new AtomicReference<>();

    withPostgresAndSchema(connection -> capturingConsume(capturedWork, connection));

    assertNull(capturedWork.get());
  }

  @Test
  void fetchesAndDistributesWorkOrderedByKeyAndVersion() throws Exception {
    AtomicReference<WorkItem> capturedWorkA = new AtomicReference<>();
    AtomicReference<WorkItem> capturedWorkB = new AtomicReference<>();
    AtomicReference<WorkItem> capturedWorkC = new AtomicReference<>();

    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch syncLatch = new CountDownLatch(1);

    Consumer advanced = Consumer.advanced();

    withPostgresConnectionsAndSchema(
        connections -> {
          Connection connection = connections.get();
          Connection anotherConnection = connections.get();

          scheduleSomeWork(connection);

          Thread worker =
              new Thread(
                  () ->
                      advanced
                          .next(
                              workItem -> {
                                capturedWorkA.set(workItem);
                                syncLatch.countDown();
                                awaitLatch(blockLatch);
                              })
                          .accept(connection));
          worker.start();

          awaitLatch(syncLatch);
          // Obtain the next free work item while the other thread holds the first.
          advancedCapturingConsume(capturedWorkB, anotherConnection);
          blockLatch.countDown();

          joinThread(worker);

          advancedCapturingConsume(capturedWorkC, anotherConnection);
        });

    assertNextWorkItemIsCaptured(capturedWorkA.get(), new WorkItem("a", "b", 23));
    assertNextWorkItemIsCaptured(capturedWorkB.get(), new WorkItem("c", "d", 12));
    assertNextWorkItemIsCaptured(capturedWorkC.get(), new WorkItem("a", "b", 24));
  }

  private void joinThread(final Thread worker) {
    try {
      worker.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void awaitLatch(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void capturingConsume(AtomicReference<WorkItem> work, Connection connection) {
    Consumer.simple().next(work::set).accept(connection);
  }

  private void advancedCapturingConsume(AtomicReference<WorkItem> work, Connection connection) {
    Consumer.advanced().next(work::set).accept(connection);
  }

  private void failingConsume(Connection connection) {
    try {
      Consumer.simple()
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
    producer.produce(new WorkItem("a", "b", 24)).apply(connection);
    producer.produce(new WorkItem("c", "d", 12)).apply(connection);
  }
}
