package net.andreaskluth.elefantenstark.setup;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class InitializerTest {

  @Test
  void initializesSchema() {
    withPostgres(
        connection -> {
          new Initializer().build(connection);
          validateTableIsAvailable(connection);
        });
  }

  private void validateTableIsAvailable(Connection connection) {
    try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM queue")) {
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
