package net.andreaskluth.elefantenstark.consumer;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresConnectionsAndSchema;
import static net.andreaskluth.elefantenstark.TestData.scheduleThreeWorkItems;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.assertNextWorkItemIsCaptured;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.capturingConsume;

import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.consumer.Consumer.WorkItemContext;
import net.andreaskluth.elefantenstark.work.WorkItem;
import org.junit.jupiter.api.Test;

class MultiThreadedConsumerTest {

  @Test
  void fetchesAndDistributesWorkOrderedByKeyAndVersionTransactionScoped() {
    validateConsumer(Consumer.transactionScoped());
  }

  @Test
  void fetchesAndDistributesWorkOrderedByKeyAndVersionSessionScoped() {
    validateConsumer(Consumer.sessionScoped());
  }

  private void validateConsumer(Consumer consumer) {
    AtomicReference<WorkItemContext> capturedWorkA = new AtomicReference<>();
    AtomicReference<WorkItemContext> capturedWorkB = new AtomicReference<>();
    AtomicReference<WorkItemContext> capturedWorkC = new AtomicReference<>();

    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch syncLatch = new CountDownLatch(1);

    withPostgresConnectionsAndSchema(
        connections -> {
          Connection connection = connections.get();
          Connection anotherConnection = connections.get();

          scheduleThreeWorkItems(connection);

          Thread worker =
              new Thread(
                  () ->
                      consumer.next(
                          connection,
                          wic -> {
                            capturedWorkA.set(wic);
                            syncLatch.countDown();
                            awaitLatch(blockLatch);
                            return null;
                          }));
          worker.start();

          awaitLatch(syncLatch);
          // Obtain the next free work item while the other thread holds the first.
          capturingConsume(anotherConnection, consumer, capturedWorkB);
          blockLatch.countDown();

          joinThread(worker);

          capturingConsume(anotherConnection, consumer, capturedWorkC);
        });

    assertNextWorkItemIsCaptured(WorkItem.groupedOnKey("a", "b", 23), capturedWorkA.get());
    assertNextWorkItemIsCaptured(WorkItem.groupedOnKey("c", "d", 12), capturedWorkB.get());
    assertNextWorkItemIsCaptured(WorkItem.groupedOnKey("a", "b", 24), capturedWorkC.get());
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
}
