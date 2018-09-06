package net.andreaskluth.elefantenstark.producer;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresAndSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.andreaskluth.elefantenstark.WorkItem;
import org.junit.jupiter.api.Test;

class ProducerTest {

  @Test
  void addsWorkItemsToQueue() throws Exception {
    withPostgresAndSchema(
        connection -> {
          WorkItem workItem = new WorkItem("_test_key_", "_test_value_", 0);
          new Producer().produce(workItem).apply(connection);

          WorkItem queuedWorkItem = queryForWorkItem(connection);

          assertEquals(workItem, queuedWorkItem);
        });
  }

  private WorkItem queryForWorkItem(Connection connection) {
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT key, value, version FROM queue")) {
      if (rs.next()) {
        return new WorkItem(rs.getString("key"), rs.getString("value"), rs.getLong("version"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }
}
