package net.andreaskluth.elefantenstark;

import net.andreaskluth.elefantenstark.hashing.CRC32Hash;

public class WorkItemGroupedOnKey extends WorkItem {

  public WorkItemGroupedOnKey(String key, String value, long version) {
    super(key, value, CRC32Hash.hash(key), version);
  }
}
