package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * PFADD operation redis mapper.
 */
public class PfAddMapper extends RowRedisMapper {

    public PfAddMapper() {
        super(RedisCommand.PFADD);
    }

}
