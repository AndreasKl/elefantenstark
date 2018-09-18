package net.andreaskluth.elefantenstark.work;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Provides a facility to store context data for a {@link WorkItem}. */
public class WorkItemDataMap {

  private static final WorkItemDataMap EMPTY = new WorkItemDataMap(Collections.emptyMap());
  private final Map<String, String> map;

  /**
   * Creates a new {@link WorkItem} based on a {@link Map} which is stored alongside the {@link
   * WorkItem}.
   *
   * @param map the context data.
   */
  public WorkItemDataMap(Map<String, String> map) {
    Map<String, String> unsafe = requireNonNull(map);
    this.map = new HashMap<>(unsafe);
  }

  /**
   * Creates a copy of the passed in {@link WorkItemDataMap}.
   *
   * @param source to create a clone form.
   */
  protected WorkItemDataMap(WorkItemDataMap source) {
    requireNonNull(source);
    this.map = new HashMap<>(source.map);
  }

  public static WorkItemDataMap empty() {
    return EMPTY;
  }

  public Map<String, String> map() {
    return map;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkItemDataMap that = (WorkItemDataMap) o;
    return Objects.equals(map, that.map);
  }

  @Override
  public int hashCode() {
    return Objects.hash(map);
  }
}
