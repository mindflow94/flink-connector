package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created on 2021/9/15.
 *
 * @author MariaCarrie
 */
public class RedisDynamicTableSinkFactory implements DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RedisDynamicTableSinkFactory.class);

    public static final String IDENTIFIER = "redis";
    public static final ConfigOption<Integer> DATABASE = ConfigOptions.key(RedisValidator.REDIS_SINGLE_DATABASE.toLowerCase())
            .intType()
            .defaultValue(Integer.valueOf(0))
            .withDescription("The data base to connect.");
    public static final ConfigOption<String> HOST = ConfigOptions.key(RedisValidator.REDIS_SINGLE_HOST.toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The host to connect.");
    public static final ConfigOption<String> PASSWARD = ConfigOptions.key(RedisValidator.REDIS_SINGLE_PASSWORD.toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The passward to connect.");
    public static final ConfigOption<Integer> PORT = ConfigOptions.key(RedisValidator.REDIS_SINGLE_PORT.toLowerCase())
            .intType()
            .defaultValue(Integer.valueOf(6379))
            .withDescription("The port to connect.");
    public static final ConfigOption<String> COMMAND = ConfigOptions.key(RedisValidator.REDIS_COMMAND.toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The redis command to excute.");
    public static final ConfigOption<String> REDIS_MODE = ConfigOptions.key(RedisValidator.REDIS_MODE.toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The redis mode to use.");
    public static final ConfigOption<String> KEY_TTL = ConfigOptions.key(RedisValidator.REDIS_KEY_TTL.toLowerCase())
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> MASTER_NAME = ConfigOptions.key(RedisValidator.REDIS_MASTER_NAME.toLowerCase())
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SENTINELS_INFO = ConfigOptions.key(RedisValidator.SENTINELS_INFO.toLowerCase())
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SENTINELS_PASSWORD = ConfigOptions.key(RedisValidator.SENTINELS_PASSWORD.toLowerCase())
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> CLUSTER_NODES = ConfigOptions.key(RedisValidator.REDIS_NODES.toLowerCase())
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> CLUSTER_PASSWORD = ConfigOptions.key(RedisValidator.REDIS_CLUSTER_PASSWORD.toLowerCase())
            .stringType()
            .noDefaultValue();

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        LOG.info("Create dynamic redis table sink: {}.", catalogOptions);
        return new RedisDynamicTableSink(catalogOptions, physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(REDIS_MODE);
        options.add(COMMAND);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(HOST);
        options.add(PASSWARD);
        options.add(PORT);
        options.add(DATABASE);
        options.add(KEY_TTL);
        options.add(SENTINELS_INFO);
        options.add(SENTINELS_PASSWORD);
        options.add(MASTER_NAME);
        options.add(CLUSTER_NODES);
        options.add(CLUSTER_PASSWORD);
        return options;
    }
}
