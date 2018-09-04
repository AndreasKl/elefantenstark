package net.andreaskluth.elefantenstark;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import net.andreaskluth.elefantenstark.setup.Initializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresSupport {

  private static final Logger log = LoggerFactory.getLogger(PostgresSupport.class);

  private PostgresSupport() {
    throw new RuntimeException("Not permitted");
  }

  private static void withPostgresConnections(
      Consumer<PostgresConnectionProvider> connectionConsumer) throws IOException {

    try (EmbeddedPostgres postgres = EmbeddedPostgres.builder().start();
        PostgresConnectionProvider provider = new PostgresConnectionProvider(postgres)) {
      connectionConsumer.accept(provider);
    }
  }

  public static void withPostgres(Consumer<Connection> postgresConnectionConsumer)
    throws IOException {

    withPostgresConnections(connections -> postgresConnectionConsumer.accept(connections.get()));
  }

  public static void withPostgresConnectionsAndSchema(
      Consumer<PostgresConnectionProvider> connectionConsumer) throws IOException {

    withPostgresConnections(
        provider -> {
          new Initializer().build().accept(provider.get());
          connectionConsumer.accept(provider);
        });
  }

  public static void withPostgresAndSchema(Consumer<Connection> postgresConnectionConsumer)
      throws IOException {

    withPostgresConnectionsAndSchema(
        connections -> postgresConnectionConsumer.accept(connections.get()));
  }

  public static class PostgresConnectionProvider implements AutoCloseable {

    private final EmbeddedPostgres embeddedPostgres;
    private final List<Connection> connections = new ArrayList<>();

    private PostgresConnectionProvider(EmbeddedPostgres embeddedPostgres) {
      this.embeddedPostgres = embeddedPostgres;
    }

    @Override
    public void close() {
      for (Connection connection : connections) {
        try {
          connection.close();
        } catch (Exception ex) {
          log.warn("Not able to close database connection. {} ", connection, ex);
        }
      }
    }

    public Connection get() {
      final Connection connection;
      try {
        connection = embeddedPostgres.getPostgresDatabase().getConnection();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      connections.add(connection);
      return connection;
    }
  }
}
