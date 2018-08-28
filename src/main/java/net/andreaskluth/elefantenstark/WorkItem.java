package net.andreaskluth.elefantenstark;

import java.util.Objects;

public class WorkItem {

    private final String key;
    private final String value;
    private final long version;

    public WorkItem(String key, String value, long version) {
        this.key = Objects.requireNonNull(key);
        this.value = Objects.requireNonNull(value);
        this.version = Objects.requireNonNull(version);
    }

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }

    public long version() {
        return version;
    }
}
