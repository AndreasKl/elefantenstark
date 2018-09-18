package net.andreaskluth.elefantenstark.work;

import com.google.protobuf.InvalidProtocolBufferException;
import net.andreaskluth.elefantenstark.work.generated.DictionaryProtos;

public class WorkItemDataMapDeserializer {

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

  public static class WorkItemDataMapDeserializerException extends RuntimeException {
    private static final long serialVersionUID = 7226506725479629459L;

    public WorkItemDataMapDeserializerException(Throwable cause) {
      super(cause);
    }
  }
}
