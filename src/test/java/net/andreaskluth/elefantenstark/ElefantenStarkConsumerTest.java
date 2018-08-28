package net.andreaskluth.elefantenstark;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static net.andreaskluth.elefantenstark.PostgresSupport.withPostgresAndSchema;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ElefantenStarkConsumerTest {

    @Test
    void fetchesAndDistributesWorkOrderedByVersion() throws Exception {
        WorkItem[] work = new WorkItem[1];
        ElefantenStarkConsumer elefantenStarkConsumer = new ElefantenStarkConsumer();

        withPostgresAndSchema(connection -> {
            scheduleWork(connection);
            elefantenStarkConsumer.next(workItem -> work[0] = workItem).accept(connection);
        });

        assertAll("work",
            () -> assertEquals("a", work[0].key()),
            () -> assertEquals("b", work[0].value()),
            () -> assertEquals(23, work[0].version())
        );
    }

    @Test
    void whenTheWorkerFailsTheWorkCanBeReConsumed() throws Exception {
        WorkItem[] work = new WorkItem[1];
        ElefantenStarkConsumer elefantenStarkConsumer = new ElefantenStarkConsumer();

        withPostgresAndSchema(connection -> {
            scheduleWork(connection);
            try {
                elefantenStarkConsumer.next(workItem -> {
                    throw new IllegalStateException();
                }).accept(connection);
            } catch (RuntimeException ex) {
                // Ignore
            }

            elefantenStarkConsumer.next(workItem -> work[0] = workItem).accept(connection);
        });

        assertAll("work",
            () -> assertEquals("a", work[0].key()),
            () -> assertEquals("b", work[0].value()),
            () -> assertEquals(23, work[0].version())
        );
    }

    @Test
    void ifThereIsNoWorkNothingIsConsumed() throws Exception {
        AtomicBoolean foundWork = new AtomicBoolean(false);
        ElefantenStarkConsumer elefantenStarkConsumer = new ElefantenStarkConsumer();

        withPostgresAndSchema(connection -> elefantenStarkConsumer.next(__ -> foundWork.set(true)).accept(connection));

        assertFalse(foundWork.get());
    }

    private void scheduleWork(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO queue (\"key\", value, version) VALUES('a', 'b', 23)");
            statement.executeUpdate("INSERT INTO queue (\"key\", value, version) VALUES('c', 'd', 12)");
            statement.executeUpdate("INSERT INTO queue (\"key\", value, version) VALUES('a', 'b', 24)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}