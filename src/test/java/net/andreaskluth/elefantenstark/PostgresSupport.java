package net.andreaskluth.elefantenstark;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;

class PostgresSupport {

    private PostgresSupport() {
        throw new RuntimeException("Not permitted");
    }

    static void withPostgres(Consumer<Connection> postgresConnectionConsumer) throws IOException, SQLException {
        try (EmbeddedPostgres start = EmbeddedPostgres.builder().start();
             Connection connection = start.getPostgresDatabase().getConnection()) {
            postgresConnectionConsumer.accept(connection);
        }
    }

    static void withPostgresAndSchema(Consumer<Connection> postgresConnectionConsumer) throws IOException, SQLException {
        withPostgres(connection -> {
                new ElefantenStarkInitializer().init().andThen(postgresConnectionConsumer).accept(connection);
            }
        );
    }
}
