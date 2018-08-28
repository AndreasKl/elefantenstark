package net.andreaskluth.elefantenstark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

public class ElefantenStarkConsumer {

    public static String OBTAIN_WORK_QUERY =
        "DELETE FROM queue " +
            "WHERE id = ( " +
            "  SELECT id " +
            "  FROM queue " +
            "  ORDER BY key, version " +
            "  FOR UPDATE SKIP LOCKED " +
            "  LIMIT 1 " +
            ")" +
            "RETURNING *;";

    /**
     * Obtains the next work item from the queue and provides the data to the @worker. If the @worker fails
     * the work is available for the next @worker.
     *
     * @param worker the worker consuming the @{@link WorkItem} to work on.
     * @return a {@link Consumer} of {@link Connection}.
     */
    public Consumer<Connection> next(Consumer<WorkItem> worker) {
        return connection -> {
            try {
                try {
                    connection.setAutoCommit(false);
                    fetchWork(connection, worker);
                    connection.commit();
                } catch (Throwable ex) {
                    connection.rollback();
                    throw ex;
                } finally {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                throw new WorkConsumerException(e);
            }
        };
    }

    private void fetchWork(Connection connection, Consumer<WorkItem> worker) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet rawWorkEntry = statement.executeQuery(OBTAIN_WORK_QUERY);
            if (!rawWorkEntry.next()) {
                return;
            }
            worker.accept(mapWorkEntryFrom(rawWorkEntry));
        }
    }

    private WorkItem mapWorkEntryFrom(ResultSet rawWorkEntry) throws SQLException {
        return new WorkItem(rawWorkEntry.getString("key"), rawWorkEntry.getString("value"), rawWorkEntry.getLong("version"));
    }

    public static class WorkConsumerException extends RuntimeException {
        WorkConsumerException(Throwable cause) {
            super(cause);
        }
    }

}
