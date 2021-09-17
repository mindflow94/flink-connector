package org.apache.flink.streaming.connectors.redis.common.mapper.row;


import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * ZADD operation redis mapper.
 */
public class ZAddMapper extends RowRedisMapper {

    public ZAddMapper() {
        super(RedisCommand.ZADD);
    }
}
