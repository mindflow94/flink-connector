package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * HSET operation redis mapper.
 */
public class HIncrByMapper extends RowRedisMapper {

    public HIncrByMapper() {
        super(RedisCommand.HINCRBY);
    }

}
