package dev.rocksqueue.ser;

public interface Serializer<T> {
    byte[] serialize(T value);
    T deserialize(byte[] bytes, Class<T> type);
}
