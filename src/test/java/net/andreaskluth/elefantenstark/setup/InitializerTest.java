package net.andreaskluth.elefantenstark.setup;

import java.sql.Connection;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgres;

class InitializerTest {

  @Test
  void initializesSchema() throws Exception {
    withPostgres(connection -> {
      Consumer<Connection> initializer = new Initializer().build();
      initializer.accept(connection);
    });
  }
}
