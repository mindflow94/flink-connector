package org.apache.flink.streaming.connectors.mongodb;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.connectors.mongodb.sink.MongodbDynamicTableSink;
import org.apache.flink.streaming.connectors.mongodb.sink.MongodbSinkConf;
import org.apache.flink.streaming.connectors.mongodb.util.ContextUtil;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class MongodbDynamicTableSinkFactory implements DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbDynamicTableSinkFactory.class);
    @VisibleForTesting
    public static final String IDENTIFIER = "mongodb";
    public static final ConfigOption<String> DATABASE = ConfigOptions.key("database".toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The data base to connect.");
    public static final ConfigOption<String> URI = ConfigOptions.key("uri".toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The uri to connect.");
    public static final ConfigOption<String> COLLECTION_NAME = ConfigOptions.key("collection".toLowerCase())
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the collection to return.");
    public static final ConfigOption<Integer> MAX_CONNECTION_IDLE_TIME = ConfigOptions.key("maxConnectionIdleTime".toLowerCase())
            .intType()
            .defaultValue(Integer.valueOf(60000))
            .withDescription("The maximum idle time for a pooled connection.");
    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions.key("batchSize".toLowerCase())
            .intType()
            .defaultValue(Integer.valueOf(1024))
            .withDescription("The batch size when sink invoking.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        // 参数小写转换
        ContextUtil.transformContext(this, context);
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 必填、选填参数校验
        helper.validate();

        MongodbSinkConf mongodbSinkConf = new MongodbSinkConf((String) helper.getOptions().get(DATABASE), (String) helper.getOptions().get(COLLECTION_NAME), (String) helper.getOptions().get(URI), ((Integer) helper.getOptions().get(MAX_CONNECTION_IDLE_TIME)).intValue(), ((Integer) helper.getOptions().get(BATCH_SIZE)).intValue());

        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        LOG.info("Create dynamic mongoDB table sink: {}.", mongodbSinkConf);
        return new MongodbDynamicTableSink(mongodbSinkConf, physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet();
        requiredOptions.add(DATABASE);
        requiredOptions.add(COLLECTION_NAME);
        requiredOptions.add(URI);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionals = new HashSet();
        optionals.add(MAX_CONNECTION_IDLE_TIME);
        optionals.add(BATCH_SIZE);
        return optionals;
    }
}
