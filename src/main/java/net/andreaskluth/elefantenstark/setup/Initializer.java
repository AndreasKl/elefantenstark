package net.andreaskluth.elefantenstark.setup;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.function.Consumer;

/** Creates the schema needed for ElefantenStark. */
public class Initializer {

  private static final String POSTGRES_SCHEMA_SQL = "postgres_schema.sql";

  public Consumer<Connection> build() {
    return connection -> {
      try (Statement statement = connection.createStatement()) {
        statement.execute(loadSchemaScript());
      } catch (SQLException e) {
        throw new DatabaseInitializationException(e);
      }
    };
  }

  protected String loadSchemaScript() {
    try (InputStream stream = getSqlSchemaResourceAsStream()) {
      Scanner scanner = new Scanner(stream, UTF_8.name()).useDelimiter("\\A");
      return scanner.hasNext() ? scanner.next() : "";
    } catch (IOException e) {
      throw new DatabaseInitializationException(e);
    }
  }

  protected InputStream getSqlSchemaResourceAsStream() {
    return Initializer.class.getClassLoader().getResourceAsStream(POSTGRES_SCHEMA_SQL);
  }

  public class DatabaseInitializationException extends RuntimeException {
    DatabaseInitializationException(Throwable cause) {
      super(cause);
    }
  }
}
