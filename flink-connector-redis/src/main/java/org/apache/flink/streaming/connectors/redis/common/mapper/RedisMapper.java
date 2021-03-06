package org.apache.flink.streaming.connectors.redis.common.mapper;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
import java.util.Optional;

/**
 * Function that creates the description how the input data should be mapped to redis type.
 * <p>Example:
 * <pre>{@code
 * private static class RedisTestMapper implements RedisMapper<Tuple2<String, String>> {
 *    public RedisDataTypeDescription getCommandDescription() {
 *        return new RedisDataTypeDescription(RedisCommand.PUBLISH);
 *    }
 *    public String getKeyFromData(Tuple2<String, String> data) {
 *        return data.f0;
 *    }
 *    public String getValueFromData(Tuple2<String, String> data) {
 *        return data.f1;
 *    }
 * }
 * }</pre>
 *
 * @param <T> The type of the element handled by this {@code RedisMapper}
 */
public interface RedisMapper<T> extends Function, Serializable {

    /**
     * Returns descriptor which defines data type.
     *
     * @return data type descriptor
     */
    RedisCommandDescription getCommandDescription();

    /**
     * Extracts key from data.
     *
     * @param data source data
     * @return key
     */
    String getKeyFromData(T data);

    /**
     * Extracts value from data.
     *
     * @param data source data
     * @return value
     */
    String getValueFromData(T data);

    /**
     * Extracts the additional key from data as an {@link Optional<String>}.
     * The default implementation returns an empty Optional.
     *
     * @param data
     * @return Optional
     */
    default Optional<String> getAdditionalKey(T data) {
        return Optional.empty();
    }

    /**
     * Extracts the additional time to live (TTL) for data as an {@link Optional<Integer>}.
     * The default implementation returns an empty Optional.
     *
     * @param data
     * @return Optional
     */
    default Optional<Integer> getAdditionalTTL(T data) {
        return Optional.empty();
    }
}
