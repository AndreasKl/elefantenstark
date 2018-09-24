package net.andreaskluth.elefantenstark;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcSupport {

  private JdbcSupport() {
    throw new UnsupportedOperationException("Not permitted");
  }

  public static void close(Connection connection) {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
