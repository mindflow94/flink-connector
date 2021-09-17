package org.apache.flink.streaming.connectors.redis.common.config.handler;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.util.Preconditions;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.*;

/**
 * jedis cluster config handler to find and create jedis cluster config use meta.
 */
public class FlinkJedisClusterConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(Map<String, String> properties) {
        Preconditions.checkArgument(properties.containsKey(REDIS_NODES), "nodes should not be null in cluster mode");
        String nodesInfo = properties.get(REDIS_NODES);
        Set<InetSocketAddress> nodes = Arrays.stream(nodesInfo.split(",")).map(r -> {
            String[] arr = r.split(":");
            return new InetSocketAddress(arr[0].trim(), Integer.parseInt(arr[1].trim()));
        }).collect(Collectors.toSet());
        String clusterPassword = properties.getOrDefault(REDIS_CLUSTER_PASSWORD, null);
        FlinkJedisClusterConfig.Builder builder = new FlinkJedisClusterConfig.Builder();
        builder.setNodes(nodes);
        if (StringUtils.isNotBlank(clusterPassword)) {
            builder.setPassword(clusterPassword);
        }
        return builder.build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_CLUSTER);
        return require;
    }

    public FlinkJedisClusterConfigHandler() {
    }
}
