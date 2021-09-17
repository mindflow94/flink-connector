package org.apache.flink.streaming.connectors.redis.common.config.handler;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.*;

/**
 * Created on 2021/9/13.
 *
 * @author MariaCarrie
 * <p>
 * jedis single config handler to find and create jedis pool config use meta.
 */
public class FlinkJedisPoolConfigHandler implements FlinkJedisConfigHandler {
    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(Map<String, String> properties) {
        Preconditions.checkArgument(properties.containsKey(REDIS_SINGLE_HOST), "host should not be null in single mode");
        String singlePassword = properties.getOrDefault(REDIS_SINGLE_PASSWORD, null);
        String singlePort = properties.getOrDefault(REDIS_SINGLE_PORT, null);
        String singleDatabase = properties.getOrDefault(REDIS_SINGLE_DATABASE, null);
        FlinkJedisPoolConfig.Builder builder = new FlinkJedisPoolConfig.Builder();
        builder.setHost(properties.get(REDIS_SINGLE_HOST));
        if (StringUtils.isNotBlank(singlePassword)) {
            builder.setPassword(singlePassword);
        }
        if (StringUtils.isNotBlank(singlePort)) {
            builder.setPort(Integer.parseInt(singlePort));
        }
        if (StringUtils.isNotBlank(singleDatabase)) {
            builder.setDatabase(Integer.parseInt(singleDatabase));
        }
        return builder.build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_SINGLE);
        return require;
    }

    public FlinkJedisPoolConfigHandler() {
    }
}
