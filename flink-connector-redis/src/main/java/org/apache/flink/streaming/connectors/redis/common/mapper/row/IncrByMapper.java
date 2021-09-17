package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * Delta plus operation
 */
public class IncrByMapper extends RowRedisMapper {

    public IncrByMapper() {
        super(RedisCommand.INCRBY);
    }

}
