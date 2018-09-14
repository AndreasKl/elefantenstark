package net.andreaskluth.elefantenstark.producer;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import net.andreaskluth.elefantenstark.WorkItem;

public class Producer {

  public static final String INSERT_WORK_QUERY =
      "INSERT INTO queue (\"key\", value, version) VALUES (?, ?, ?)";

  public boolean produce(Connection connection, WorkItem workItem) {
    requireNonNull(connection);
    requireNonNull(workItem);

    try (PreparedStatement statement = connection.prepareStatement(INSERT_WORK_QUERY)) {
      statement.setString(1, workItem.key());
      statement.setString(2, workItem.value());
      statement.setLong(3, workItem.version());
      return statement.execute();
    } catch (SQLException e) {
      throw new ProducerException(e);
    }
  }

  private class ProducerException extends RuntimeException {
    public ProducerException(Throwable cause) {
      super(cause);
    }
  }
}
