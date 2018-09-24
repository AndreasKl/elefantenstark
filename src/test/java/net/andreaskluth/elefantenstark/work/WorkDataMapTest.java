package net.andreaskluth.elefantenstark.work;

import static java.util.Collections.singletonMap;
import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresAndSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import net.andreaskluth.elefantenstark.consumer.Consumer;
import net.andreaskluth.elefantenstark.consumer.ConsumerTestSupport;
import net.andreaskluth.elefantenstark.producer.Producer;
import org.junit.jupiter.api.Test;

class WorkDataMapTest {

  private static final WorkItemDataMap WORK_ITEM_DATA_MAP =
      new WorkItemDataMap(singletonMap("make me feel", "invincible"));
  private static final WorkItem WORK_ITEM =
      WorkItem.hashedOnKey("_key_", "_value_", 0, WORK_ITEM_DATA_MAP);

  private static final Producer PRODUCER = new Producer();
  private static final Consumer CONSUMER = Consumer.transactionScoped();

  @Test
  void workDataMapIsTransferredOverQueue() {
    AtomicReference<WorkItemContext> capture = new AtomicReference<>();
    withPostgresAndSchema(
        connection -> {
          PRODUCER.produce(connection, WORK_ITEM);
          ConsumerTestSupport.capturingConsume(connection, CONSUMER, capture);
        });

    assertEquals(WORK_ITEM_DATA_MAP, capture.get().workItem().workItemDataMap());
  }
}
