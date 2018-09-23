package net.andreaskluth.elefantenstark.work;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import net.andreaskluth.elefantenstark.hashing.CRC32Hash;

/** Item to provide and consume data to work on. */
public class WorkItem {

  private final String key;
  private final String value;
  private final int hash;
  private final long version;
  private final WorkItemDataMap dataMap;

  /**
   * Creates a new instance of {@link WorkItem} used to provide work to a {@link
   * net.andreaskluth.elefantenstark.producer.Producer}.
   *
   * @param key the business key
   * @param value the payload value
   * @param hash the hash this {@link WorkItem} belongs to, is used to for locking.
   * @param version the version of this {@link WorkItem} e.g. currently used for informational
   *     purposes only.
   * @param dataMap a map of additional context data
   */
  public WorkItem(String key, String value, int hash, long version, WorkItemDataMap dataMap) {
    this.key = requireNonNull(key);
    this.value = requireNonNull(value);

    this.hash = hash;
    this.version = version;

    WorkItemDataMap source = requireNonNull(dataMap);
    this.dataMap = new WorkItemDataMap(source);
  }

  /**
   * Creates a {@link WorkItem} with a {@link WorkItem#hash()} based on {@link WorkItem#key()}
   * calculated with a cheap {@link CRC32Hash}.
   *
   * @param key the business key
   * @param value the payload value
   * @param version the version of this entity
   * @return the {@link WorkItem} grouped by its key.
   */
  public static WorkItem hashedOnKey(String key, String value, long version) {
    return hashedOnKey(key, value, version, WorkItemDataMap.empty());
  }

  /**
   * Creates a {@link WorkItem} with a {@link WorkItem#hash()} based on {@link WorkItem#key()}
   * calculated with a cheap {@link CRC32Hash}.
   *
   * @param key the business key
   * @param value the payload value
   * @param version the version of this entity
   * @param workItemDataMap the context data that will be passed to the {@link
   *     net.andreaskluth.elefantenstark.consumer.Consumer}.
   * @return the {@link WorkItem} grouped by its key.
   */
  public static WorkItem hashedOnKey(
      String key, String value, long version, WorkItemDataMap workItemDataMap) {
    return new WorkItem(key, value, CRC32Hash.hash(key), version, workItemDataMap);
  }

  public String key() {
    return key;
  }

  public int hash() {
    return hash;
  }

  public String value() {
    return value;
  }

  public long version() {
    return version;
  }

  public WorkItemDataMap workItemDataMap() {
    return dataMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WorkItem)) {
      return false;
    }
    WorkItem workItem = (WorkItem) o;
    return version == workItem.version
        && Objects.equals(key, workItem.key)
        && Objects.equals(value, workItem.value)
        && Objects.equals(hash, workItem.hash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, version);
  }

  @Override
  public String toString() {
    return "WorkItem{key='"
        + key
        + "', value='"
        + value
        + "', hash="
        + hash
        + ", version="
        + version
        + "}";
  }
}
