package net.andreaskluth.elefantenstark.hashing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class CRC32HashTest {

  @Test
  void checksumIsCalculated() {
    assertEquals(-129305979, CRC32Hash.hash("中餐很好吃"));
    assertEquals(1344803957, CRC32Hash.hash("Hallo Welt"));
  }
}
