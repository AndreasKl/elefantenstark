package net.andreaskluth.elefantenstark.producer;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresAndSchema;
import static net.andreaskluth.elefantenstark.work.WorkItemDataMapDeserializer.deserialize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.andreaskluth.elefantenstark.work.WorkItem;
import org.junit.jupiter.api.Test;

class ProducerTest {

  @Test
  void addsWorkItemsToQueue() {
    withPostgresAndSchema(
        connection -> {
          WorkItem workItem = WorkItem.groupedOnKey("_test_key_", "_test_value_", 0);
          new Producer().produce(connection, workItem);

          WorkItem queuedWorkItem = queryForWorkItem(connection);

          assertEquals(workItem, queuedWorkItem);
        });
  }

  private WorkItem queryForWorkItem(Connection connection) {
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT key, value, \"group\", version, data_map FROM queue")) {
      if (rs.next()) {
        return new WorkItem(
            rs.getString("key"),
            rs.getString("value"),
            rs.getInt("group"),
            rs.getLong("version"),
            deserialize(rs.getBytes("data_map")));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }
}
