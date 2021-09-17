package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * LPUSH operation redis mapper.
 */
public class LPushMapper extends RowRedisMapper {

    public LPushMapper() {
        super(RedisCommand.LPUSH);
    }

}
