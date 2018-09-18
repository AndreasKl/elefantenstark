package net.andreaskluth.elefantenstark.work;

import net.andreaskluth.elefantenstark.work.generated.DictionaryProtos;

public class WorkItemDataMapSerializer {

  /**
   * Serializes a {@link WorkItemDataMap} to a protobuf map.
   *
   * @param workItemDataMap the context map to serialize
   * @return the serialized form
   */
  public static byte[] serialize(WorkItemDataMap workItemDataMap) {
    return DictionaryProtos.Dictionary.newBuilder()
        .putAllPairs(workItemDataMap.map())
        .build()
        .toByteArray();
  }
}
