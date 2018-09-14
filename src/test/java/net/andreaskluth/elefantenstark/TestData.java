package net.andreaskluth.elefantenstark;

import java.sql.Connection;
import net.andreaskluth.elefantenstark.producer.Producer;

public class TestData {

  public static void scheduleThreeWorkItems(Connection connection) {
    Producer producer = new Producer();
    producer.produce(connection, new WorkItem("a", "b", 23));
    producer.produce(connection, new WorkItem("a", "b", 24));
    producer.produce(connection, new WorkItem("c", "d", 12));
  }
}
