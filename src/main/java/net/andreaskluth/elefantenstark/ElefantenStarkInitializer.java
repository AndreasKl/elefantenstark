package net.andreaskluth.elefantenstark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

/**
 * Creates the schema needed for ElefantenStark.
 */
public class ElefantenStarkInitializer {

    private static final String POSTGRES_SCHEMA_SQL = "postgres_schema.sql";

    public Consumer<Connection> init() {
        return connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute(loadSchemaScript());
            } catch (SQLException e) {
                throw new DatabaseInitializerException(e);
            }
        };
    }

    private String loadSchemaScript() {
        try (InputStream stream = ElefantenStarkInitializer.class.getClassLoader().getResourceAsStream(POSTGRES_SCHEMA_SQL)) {
            java.util.Scanner s = new java.util.Scanner(stream, StandardCharsets.UTF_8.name()).useDelimiter("\\A");
            return s.hasNext() ? s.next() : "";
        } catch (IOException e) {
            throw new DatabaseInitializerException(e);
        }
    }

    public class DatabaseInitializerException extends RuntimeException {
        DatabaseInitializerException(Throwable cause) {
            super(cause);
        }
    }

}
