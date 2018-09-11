package net.andreaskluth.elefantenstark.consumer;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresConnectionsAndSchema;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.assertNextWorkItemIsCaptured;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.capturingConsume;
import static net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport.scheduleSomeWork;

import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.WorkItem;
import org.junit.jupiter.api.Test;

class MultiThreadedConsumerTest {

  @Test
  void fetchesAndDistributesWorkOrderedByKeyAndVersionTransactionScoped() throws Exception {
    validateConsumer(Consumer.transactionScoped());
  }

  @Test
  void fetchesAndDistributesWorkOrderedByKeyAndVersionSessionScoped() throws Exception {
    validateConsumer(Consumer.sessionScoped());
  }

  private void validateConsumer(Consumer consumer) throws IOException {
    AtomicReference<WorkItem> capturedWorkA = new AtomicReference<>();
    AtomicReference<WorkItem> capturedWorkB = new AtomicReference<>();
    AtomicReference<WorkItem> capturedWorkC = new AtomicReference<>();

    CountDownLatch blockLatch = new CountDownLatch(1);
    CountDownLatch syncLatch = new CountDownLatch(1);

    withPostgresConnectionsAndSchema(
        connections -> {
          Connection connection = connections.get();
          Connection anotherConnection = connections.get();

          scheduleSomeWork(connection);

          Thread worker =
              new Thread(
                  () ->
                      consumer.next(
                          connection,
                          workItem -> {
                            capturedWorkA.set(workItem);
                            syncLatch.countDown();
                            awaitLatch(blockLatch);
                          }));
          worker.start();

          awaitLatch(syncLatch);
          // Obtain the next free work item while the other thread holds the first.
          capturingConsume(anotherConnection, consumer, capturedWorkB);
          blockLatch.countDown();

          joinThread(worker);

          capturingConsume(anotherConnection, consumer, capturedWorkC);
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
}
