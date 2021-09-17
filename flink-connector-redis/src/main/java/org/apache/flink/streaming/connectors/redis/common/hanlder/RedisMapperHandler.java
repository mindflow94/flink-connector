package org.apache.flink.streaming.connectors.redis.common.hanlder;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_KEY_TTL;

/**
 * handler for create redis mapper.
 */
public interface RedisMapperHandler extends RedisHandler {

    Logger LOGGER = LoggerFactory.getLogger(RedisMapperHandler.class);

    /**
     * create a correct redis mapper use properties.
     *
     * @param properties to create redis mapper.
     * @return redis mapper.
     */
    default RedisMapper createRedisMapper(Map<String, String> properties) {
        String ttl = properties.get(REDIS_KEY_TTL);
        try {
            Class redisMapper = Class.forName(this.getClass().getCanonicalName());

            if (ttl == null) {
                return (RedisMapper) redisMapper.newInstance();
            }
            Constructor c = redisMapper.getConstructor(Integer.class);
            return (RedisMapper) c.newInstance(Integer.parseInt(ttl));
        } catch (Exception e) {
            LOGGER.error("create redis mapper failed", e);
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
