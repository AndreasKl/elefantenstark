package net.andreaskluth.elefantenstark;

import java.util.Objects;

public class WorkItem {

  private final String key;
  private final String value;
  private final int group;
  private final long version;

  public WorkItem(String key, String value, int group, long version) {
    this.key = Objects.requireNonNull(key);
    this.value = Objects.requireNonNull(value);
    this.group = group;
    this.version = version;
  }

  public String key() {
    return key;
  }

  public int group() {
    return group;
  }

  public String value() {
    return value;
  }

  public long version() {
    return version;
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
        && Objects.equals(value, workItem.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, version);
  }

  @Override
  public String toString() {
    return "WorkItem{key='" + key + "', value='" + value + "', version=" + version + "}";
  }
}
