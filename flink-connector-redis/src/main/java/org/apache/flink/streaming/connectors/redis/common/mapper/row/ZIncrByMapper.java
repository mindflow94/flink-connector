package org.apache.flink.streaming.connectors.redis.common.mapper.row;


import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * ZINCRBY operation redis mapper.
 */
public class ZIncrByMapper extends RowRedisMapper {

    public ZIncrByMapper() {
        super(RedisCommand.ZINCRBY);
    }
}
