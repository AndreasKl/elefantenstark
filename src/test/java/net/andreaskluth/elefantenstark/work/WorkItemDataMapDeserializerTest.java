package net.andreaskluth.elefantenstark.work;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class WorkItemDataMapDeserializerTest {

  @Test
  void raisesOnInvalidContent() {
    Executable scope = () -> WorkItemDataMapDeserializer.deserialize(new byte[] {1, 2, 3});
    assertThrows(WorkItemDataMapDeserializerException.class, scope);
  }

  @Test
  void serializesAndDeserializesAFilledMap() {
    byte[] value = WorkItemDataMapSerializer.serialize(new WorkItemDataMap(aMap()));

    WorkItemDataMap deserialize = WorkItemDataMapDeserializer.deserialize(value);

    assertEquals(deserialize.map(), aMap());
  }

  private Map<String, String> aMap() {
    Map<String, String> values = new HashMap<>();
    values.put("demo", "stuff");
    values.put("more", "stuff");
    values.put("other", "stuff");
    return values;
  }
}
