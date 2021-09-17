package org.apache.flink.streaming.connectors.redis.descriptor;

/**
 * redis validator for validate redis descriptor.
 */
public class RedisValidator {
    public static final String REDIS = "redis";
    public static final String REDIS_COMMAND = "command";
    public static final String REDIS_KEY_TTL = "key.ttl";
    public static final String REDIS_MODE = "redis.mode";
    public static final String REDIS_SINGLE = "single";
    public static final String REDIS_SENTINEL = "sentinel";
    public static final String REDIS_CLUSTER = "cluster";

    public static final String REDIS_SINGLE_HOST = "single.host";
    public static final String REDIS_SINGLE_PORT = "single.port";
    public static final String REDIS_SINGLE_DATABASE = "single.db";
    public static final String REDIS_SINGLE_PASSWORD = "single.password";

    public static final String REDIS_MASTER_NAME = "master.name";
    public static final String SENTINELS_INFO = "sentinels.info";
    public static final String SENTINELS_PASSWORD = "sentinels.password";

    public static final String REDIS_NODES = "cluster.nodes";
    public static final String REDIS_CLUSTER_PASSWORD = "cluster.password";


}
