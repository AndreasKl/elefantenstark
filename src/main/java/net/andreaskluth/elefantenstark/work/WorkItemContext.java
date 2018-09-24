package net.andreaskluth.elefantenstark.work;

import static java.util.Objects.requireNonNull;

/** A simple context object containing information related to a {@link WorkItem}. */
public class WorkItemContext {

  private final int id;
  private final int timesProcessed;
  private final WorkItem workItem;

  /**
   * Creates a context object containing information related to a {@link WorkItem}.
   *
   * @param id the technical id
   * @param timesProcessed the times the containing {@link WorkItem} was processed
   * @param workItem the actual {@link WorkItem}
   */
  public WorkItemContext(int id, int timesProcessed, WorkItem workItem) {
    this.id = id;
    this.timesProcessed = timesProcessed;
    this.workItem = requireNonNull(workItem);
  }

  /** @return the technical database id. */
  public int id() {
    return id;
  }

  /** @return how many times the containing {@link WorkItem} was processed. */
  public int timesProcessed() {
    return timesProcessed;
  }

  /** @return the actual {@link WorkItem}. */
  public WorkItem workItem() {
    return workItem;
  }
}
