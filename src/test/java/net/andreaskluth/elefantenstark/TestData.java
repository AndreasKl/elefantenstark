package net.andreaskluth.elefantenstark;

import java.sql.Connection;
import net.andreaskluth.elefantenstark.producer.Producer;
import net.andreaskluth.elefantenstark.work.WorkItem;

public class TestData {

  public static void scheduleThreeWorkItems(Connection connection) {
    Producer producer = new Producer();
    producer.produce(connection, WorkItem.hashedOnKey("a", "b", 23));
    producer.produce(connection, WorkItem.hashedOnKey("a", "b", 24));
    producer.produce(connection, WorkItem.hashedOnKey("c", "d", 12));
  }
}
