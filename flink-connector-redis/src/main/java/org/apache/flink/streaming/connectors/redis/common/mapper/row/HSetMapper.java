package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * HSET operation redis mapper.
 */
public class HSetMapper extends RowRedisMapper {

    public HSetMapper() {
        super(RedisCommand.HSET);
    }

}
