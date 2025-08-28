package dev.rocksqueue.ser;

import java.nio.charset.StandardCharsets;

/**
 * Lightweight serializer for String values using raw UTF-8 bytes.
 * Avoids JSON overhead for throughput benchmarks.
 */
public class Utf8StringSerializer implements Serializer<String> {
    @Override
    public byte[] serialize(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String deserialize(byte[] bytes, Class<String> type) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
