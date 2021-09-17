package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * Created on 2021/9/15.
 *
 * @author MariaCarrie
 */
public class RedisDynamicTableSink implements DynamicTableSink {

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisMapper redisMapper;
    private Map<String, String> properties = null;
    private final TableSchema tableSchema;
    private boolean isAppendOnly = true;

    public RedisDynamicTableSink(Map<String, String> properties, TableSchema tableSchema) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        redisMapper = RedisHandlerServices
                .findRedisHandler(RedisMapperHandler.class, properties)
                .createRedisMapper(properties);
        flinkJedisConfigBase = RedisHandlerServices
                .findRedisHandler(FlinkJedisConfigHandler.class, properties)
                .createFlinkJedisConfig(properties);
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter = context.createDataStructureConverter(this.tableSchema.toRowDataType());
        RedisSink<RowData> stringRedisSink = new RedisSink<>(flinkJedisConfigBase, redisMapper, converter, isAppendOnly);
        return SinkFunctionProvider.of(stringRedisSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(this.properties, this.tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "Redis";
    }
}
