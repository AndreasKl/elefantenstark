package net.andreaskluth.elefantenstark.maintenance;

import static java.sql.Timestamp.from;
import static java.util.Objects.requireNonNull;
import static net.andreaskluth.elefantenstark.consumer.ConsumerQueries.SESSION_SCOPED_UNLOCK_ADVISORY_LOCK;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;

/** Provides means to cleanup work that was worked on and release leftover locks. */
public class Henchman {

  private static Calendar getUTCCalendar() {
    return Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));
  }

  /**
   * Removes old work items from the queue.
   *
   * @param connection the connection to remove work from.
   * @param olderThan only work older than the given {@link Duration} is removed.
   */
  public void cleanupOldWorkItems(Connection connection, Duration olderThan) {
    requireNonNull(connection);
    requireNonNull(olderThan);

    Instant olderThanDate = Instant.now().minus(olderThan);
    try (PreparedStatement unlockStatement =
        connection.prepareStatement("DELETE FROM queue WHERE processed AND updated <= ?;")) {
      unlockStatement.setTimestamp(1, from(olderThanDate), getUTCCalendar());
      unlockStatement.execute();
    } catch (SQLException e) {
      throw new HenchmanException(e);
    }
  }

  /**
   * <b>handle with care</b> releases the lock taken for the hash on the queue immediately. Can
   * result in reordering and work being processed two times. If a consumer is currently running.
   *
   * @param connection the connection the locks are released from.
   * @param hash the lock to release
   */
  public void unlockAdvisoryLock(Connection connection, int hash) {
    requireNonNull(connection);

    try (PreparedStatement unlockStatement =
        connection.prepareStatement(SESSION_SCOPED_UNLOCK_ADVISORY_LOCK)) {
      unlockStatement.setInt(1, hash);
      unlockStatement.execute();
    } catch (SQLException e) {
      throw new HenchmanException(e);
    }
  }

  /**
   * <b>handle with care</b> releases all locks taken for the queue immediately. Can result in
   * reordering and work being processed two times. If a consumer is currently running.
   *
   * @param connection the connection the locks are released from.
   */
  public void unlockAllAdvisoryLocks(Connection connection) {
    requireNonNull(connection);

    try (PreparedStatement unlockStatement =
        connection.prepareStatement("SELECT pg_advisory_unlock_all();")) {
      unlockStatement.execute();
    } catch (SQLException e) {
      throw new HenchmanException(e);
    }
  }
}
