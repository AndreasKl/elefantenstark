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
        ResultSet result = statement.executeQuery("SELECT key, value, version FROM queue")) {
      if (result.next()) {
        return new WorkItem(
            result.getString("key"), result.getString("value"), result.getLong("version"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }
}
