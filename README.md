# Flink SQL Connector MongoDB 开发指南

## 背景

* 因公司业务发展，需要将大量数据通过 Flink SQL 推送到 MongoDB 中，目前 Flink 官方并未相应的 Connector 可以使用，网上也未找到完整的开发代码。
* **[bahir-flink](https://github.com/apache/bahir-flink)** 上维护了很多 Flink 官方没有的 Connector，如果需要自定义连接器开发，可以先参考此代码库。
* **[Ververica](https://developer.aliyun.com/article/724113)** 作为阿里云 Flink 企业版，也维护了大量的 Connector，可以通过查看 **[Ververica-Connector](https://mvnrepository.com/search?q=Ververica)** 的 maven 仓库，获取相应的 Connector。不过，此 Connector 会有一些自定义日志采集、运行 Metrics 采集等相关逻辑，需自行更改。本文基于此进行修改。

## 代码示例

> 完整代码，可以参考 **[flink-connector-mongodb](https://github.com/mindflow94/flink-connector)** ，本文仅给出示例。

- **pom文件**

```.xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>flink-connector</artifactId>
        <groupId>org.example</groupId>
        <version>1.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>flink-connector-mongodb_${scala.binary.version}</artifactId>
    <name>flink-connector-mongodb</name>
    <packaging>jar</packaging>

    <properties>
        <mongo.driver.version>3.12.6</mongo.driver.version>
        <mongo.driver.core.version>4.1.0</mongo.driver.core.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java-bridge_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-walkthrough-common_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner-blink_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-json</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver</artifactId>
            <version>${mongo.driver.version}</version>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>bson</artifactId>
            <version>${mongo.driver.core.version}</version>
        </dependency>
    </dependencies>

    <build>
        <finalName>flink-connector-mongodb_${scala.binary.version}</finalName>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>${maven.plugin}</version>
                <executions>
                    <execution>
                        <id>shade-flink</id>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <shadeTestJar>false</shadeTestJar>
                            <artifactSet>
                                <includes>
                                    <include>*:*</include>
                                </includes>
                            </artifactSet>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

</project>
```

- **MongodbSinkFunction 建立连接，插入数据**

```.java
public abstract class MongodbBaseSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    private final MongodbSinkConf mongodbSinkConf;
    private transient MongoClient client;
    private transient List<Document> batch;

    protected MongodbBaseSinkFunction(MongodbSinkConf mongodbSinkConf) {
        this.mongodbSinkConf = mongodbSinkConf;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.client = new MongoClient(new MongoClientURI(this.mongodbSinkConf.getUri(), getOptions(this.mongodbSinkConf.getMaxConnectionIdleTime())));
        this.batch = new ArrayList();
    }

    private MongoClientOptions.Builder getOptions(int maxConnectionIdleTime) {
        MongoClientOptions.Builder optionsBuilder = new MongoClientOptions.Builder();
        optionsBuilder.maxConnectionIdleTime(maxConnectionIdleTime);
        return optionsBuilder;
    }

    @Override
    public void close() throws Exception {
        flush();
        super.close();
        this.client.close();
        this.client = null;
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        this.batch.add(invokeDocument(value, context));
        if (this.batch.size() >= this.mongodbSinkConf.getBatchSize()) {
            flush();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {
    }

    private void flush() {
        if (this.batch.isEmpty()) {
            return;
        }
        MongoDatabase mongoDatabase = this.client.getDatabase(this.mongodbSinkConf.getDatabase());

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(this.mongodbSinkConf.getCollection());
        mongoCollection.insertMany(this.batch);

        this.batch.clear();
    }

    abstract Document invokeDocument(IN paramIN, Context paramContext) throws Exception;
}

public class MongodbUpsertSinkFunction extends MongodbBaseSinkFunction<RowData> {
    private final DynamicTableSink.DataStructureConverter converter;
    private final String[] fieldNames;

    public MongodbUpsertSinkFunction(MongodbSinkConf mongodbSinkConf, String[] fieldNames, DynamicTableSink.DataStructureConverter converter) {
        super(mongodbSinkConf);
        this.fieldNames = fieldNames;
        this.converter = converter;
    }

    /**
     * 将二进制RowData转换成flink可处理的Row，再将Row封装成要插入的Document对象
     *
     * @param value
     * @param context
     * @return
     */
    @Override
    Document invokeDocument(RowData value, Context context) {
        Row row = (Row) this.converter.toExternal(value);
        Map<String, Object> map = new HashMap();
        for (int i = 0; i < this.fieldNames.length; i++) {
            map.put(this.fieldNames[i], row.getField(i));
        }
        return new Document(map);
    }
}
```

- **MongodbDynamicTableSink 获取 Schema 信息、数据结构转换器**

```.java
public class MongodbDynamicTableSink implements DynamicTableSink {
    private final MongodbSinkConf mongodbSinkConf;
    private final TableSchema tableSchema;

    public MongodbDynamicTableSink(MongodbSinkConf mongodbSinkConf, TableSchema tableSchema) {
        this.mongodbSinkConf = mongodbSinkConf;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        // I、-U、+U、D
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // 初始化数据结构转换器，可以将二进制的数据转换成flink可操作的Row
        DataStructureConverter converter = context.createDataStructureConverter(this.tableSchema.toRowDataType());
        return SinkFunctionProvider.of(new MongodbUpsertSinkFunction(this.mongodbSinkConf, this.tableSchema.getFieldNames(), converter));
    }

    @Override
    public DynamicTableSink copy() {
        return new MongodbDynamicTableSink(this.mongodbSinkConf, this.tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }
}
```

- **MongodbDynamicTableSinkFactory 参数定义、校验**

```.java
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
```

- **[SPI](https://cloud.tencent.com/developer/article/1475801)** 机制动态加载插件

```
// 在 resources 目录下新建 META-INF.services 目录
文件名：
org.apache.flink.table.factories.Factory
文件内容：
org.apache.flink.streaming.connectors.mongodb.MongodbDynamicTableSinkFactory
```

- **参数说明**

<table class="table table-bordered" style="width: 95%">
    <thead>
        <tr>
          <th class="text-center" style="width: 20%">参数</th>
          <th class="text-center" style="width: 20%">说明</th>
		  <th class="text-center" style="width: 20%">是否必填</th>
		  <th class="text-center" style="width: 30%">备注</th>
        </tr>
      </thead>
      <tbody>
        <tr>
            <td>connector</td><td>Connector类型</td><td>是</td><td>固定值为mongodb</td>
        </tr>
        <tr>
			<td>database</td><td>数据库名称</td><td>是</td><td>无</td>
        </tr>
        <tr>
			<td>collection</td><td>数据集合</td><td>是</td><td>无</td>
        </tr>
		<tr>
			<td>uri</td><td>MongoDB连接串</td><td>是</td><td>格式：mongodb://userName:password@host:port/?authSource=databaseName</td>
        </tr>
		<tr>
			<td>maxConnectionIdleTime</td><td>连接超时时长</td><td>否</td><td>整型值，不能为负数，单位为毫秒。</td>
        </tr>
		<tr>
		<td>batchSize</td><td>每次批量写入的条数</td><td>否</td><td>整型值。系统会设定一个大小为batchSize的缓冲条数，当数据的条数达到batchSize时，触发数据的输出。</td>
        </tr>
      </tbody>
</table>

- **调试**

```.java
    public static void main(String args[]) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        String sourceSql = "CREATE TABLE datagen (\n" +
                " id INT,\n" +
                " name STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.name.length'='10'\n" +
                ")";
        String sinkSql = "CREATE TABLE mongoddb (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'mongodb',\n" +
                "  'database'='mongoDBTest',\n" +
                "  'collection'='flink_test',\n" +
                "  'uri'='mongodb://hzy:hzy@172.0.0.1:27017/?authSource=mongoDBTest',\n" +
                "  'maxConnectionIdleTime'='20000',\n" +
                "  'batchSize'='1'\n" +
                ")";
        String insertSql = "insert into mongoddb " +
                "select id,name " +
                "from datagen";

        tableEnvironment.executeSql(sourceSql);
        tableEnvironment.executeSql(sinkSql);
        tableEnvironment.executeSql(insertSql);
    }
```

```.xml
<dependencies>
        <dependency>
            <groupId>org.example</groupId>
            <artifactId>flink-connector-mongodb_${scala.binary.version}</artifactId>
            <version>1.0-SNAPSHOT</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java-bridge_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner-blink_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-json</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime-web_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
    </dependencies>
```

- **使用**

```
1. API 方式，引入 pom 依赖即可
<dependency>
    <groupId>org.example</groupId>
    <artifactId>flink-connector-mongodb_${scala.binary.version}</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
2. SQL 客户端方式，执行以下命令打包，将jar包放置到lib目录下
mvn clean install
```

## 参考

* [github.com/apache/bahir-flink](https://github.com/apache/bahir-flink)
* [github.com/mongo-flink/mongo-flink](https://github.com/mongo-flink/mongo-flink)
* [Ververica Platform-阿里巴巴全新Flink企业版揭秘](https://developer.aliyun.com/article/724113)
* [Java SPI机制的运行原理](https://cloud.tencent.com/developer/article/1475801)





