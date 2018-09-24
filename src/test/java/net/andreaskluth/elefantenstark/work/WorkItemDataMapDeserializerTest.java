package net.andreaskluth.elefantenstark.work;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class WorkItemDataMapDeserializerTest {

  @Test
  void raisesOnInvalidContent() {
    Executable scope = () -> WorkItemDataMapDeserializer.deserialize(new byte[] {1, 2, 3});
    assertThrows(WorkItemDataMapDeserializerException.class, scope);
  }

  @ParameterizedTest
  @MethodSource("maps")
  void serializesAndDeserializesMap(Map<String, String> mapToSerialize) {
    byte[] value = WorkItemDataMapSerializer.serialize(new WorkItemDataMap(mapToSerialize));

    WorkItemDataMap deserialize = WorkItemDataMapDeserializer.deserialize(value);

    assertEquals(deserialize, new WorkItemDataMap(mapToSerialize));
  }

  private static Stream<Map<String, String>> maps() {
    return Stream.of(aMap(), Collections.emptyMap());
  }

  private static Map<String, String> aMap() {
    Map<String, String> values = new HashMap<>();
    values.put("demo", "stuff");
    values.put("more", "stuff");
    values.put("other", "stuff");
    return values;
  }
}
