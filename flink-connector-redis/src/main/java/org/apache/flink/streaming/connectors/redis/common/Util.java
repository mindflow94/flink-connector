package org.apache.flink.streaming.connectors.redis.common;

public class Util {
    public static void checkArgument(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
