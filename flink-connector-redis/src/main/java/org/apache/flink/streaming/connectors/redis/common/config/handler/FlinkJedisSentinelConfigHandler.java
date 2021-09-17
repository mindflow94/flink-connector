package org.apache.flink.streaming.connectors.redis.common.config.handler;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;

import java.util.*;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.*;

public class FlinkJedisSentinelConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(Map<String, String> properties) {
        String masterName = properties.computeIfAbsent(REDIS_MASTER_NAME, null);
        String sentinelsInfo = properties.computeIfAbsent(SENTINELS_INFO, null);
        Objects.requireNonNull(masterName, "master should not be null in sentinel mode");
        Objects.requireNonNull(sentinelsInfo, "sentinels should not be null in sentinel mode");
        Set<String> sentinels = new HashSet<>(Arrays.asList(sentinelsInfo.split(",")));
        String sentinelsPassword = properties.computeIfAbsent(SENTINELS_PASSWORD, null);
        if (sentinelsPassword != null && sentinelsPassword.trim().isEmpty()) {
            sentinelsPassword = null;
        }
        FlinkJedisSentinelConfig flinkJedisSentinelConfig = new FlinkJedisSentinelConfig.Builder()
                .setMasterName(masterName).setSentinels(sentinels).setPassword(sentinelsPassword)
                .build();
        return flinkJedisSentinelConfig;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_SENTINEL);
        return require;
    }

    public FlinkJedisSentinelConfigHandler() {

    }
}
