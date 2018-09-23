package net.andreaskluth.elefantenstark.maintenance;

import java.sql.Connection;
import java.time.Duration;

/** Provides means to cleanup work that was worked on and release leftover locks. */
public class Henchman {

  /**
   * Removes old work items from the queue.
   * @param connection the connection to remove work from.
   * @param olderThan only work older than the given {@link Duration} is removed.
   */
  public void cleanupOldWorkItems(Connection connection, Duration olderThan) {
    throw new IllegalStateException("Not yet implemented");
  }

  /**
   * <b>handle with care</b> releases the lock taken for the hash on the queue immediately. Can
   * result in reordering and work being processed two times. If a consumer is currently running.
   *
   * @param connection the connection the locks are released from.
   * @param group the lock to release
   */
  public void unlockAdvisoryLock(Connection connection, String group) {
    throw new IllegalStateException("Not yet implemented");
  }

  /**
   * <b>handle with care</b> releases all locks taken for the queue immediately. Can result in
   * reordering and work being processed two times. If a consumer is currently running.
   *
   * @param connection the connection the locks are released from.
   */
  public void unlockAllAdvisoryLocks(Connection connection) {
    throw new IllegalStateException("Not yet implemented");
  }
}
