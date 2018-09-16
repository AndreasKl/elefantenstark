package net.andreaskluth.elefantenstark.hashing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CRC32Hash {

  private CRC32Hash() {
    throw new UnsupportedOperationException("Not permitted");
  }

  public static int hash(String value) {
    requireNonNull(value);

    java.util.zip.CRC32 hash = new java.util.zip.CRC32();
    hash.update(value.getBytes(UTF_8));
    return (int) hash.getValue();
  }
}
