package net.andreaskluth.elefantenstark.work;

import com.google.protobuf.InvalidProtocolBufferException;
import net.andreaskluth.elefantenstark.work.generated.DictionaryProtos;

public class WorkItemDataMapDeserializer {

  /**
   * Deserializes a {@link WorkItemDataMap} from its serialized form.
   *
   * @param source the byte[] to deserialize.
   * @return the {@link WorkItemDataMap}
   * @throws WorkItemDataMapDeserializerException if the byte[] passed to {@link
   *     #deserialize(byte[])} can not be deserialized.
   */
  public static WorkItemDataMap deserialize(byte[] source) {
    if (source.length == 0) {
      return WorkItemDataMap.empty();
    }

    final DictionaryProtos.Dictionary dictionary;
    try {
      dictionary = DictionaryProtos.Dictionary.parseFrom(source);
    } catch (InvalidProtocolBufferException ex) {
      throw new WorkItemDataMapDeserializerException(ex);
    }
    return new WorkItemDataMap(dictionary.getPairsMap());
  }
}
