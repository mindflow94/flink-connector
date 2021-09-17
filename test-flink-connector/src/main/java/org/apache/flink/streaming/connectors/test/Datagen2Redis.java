package org.apache.flink.streaming.connectors.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class Datagen2Redis {
    public static void main(final String[] args) {
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
        String sinkSql = "CREATE TABLE redis (\n" +
                "  name STRING,\n" +
                "  id INT\n" +
                ") WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'redis.mode'='single',\n" +
                "  'command'='SETEX',\n" +
                "  'single.host'='172.0.0.1',\n" +
                "  'single.port'='6379',\n" +
                "  'single.db'='0',\n" +
                "  'key.ttl' = '60',\n" +
                "  'single.password'='password'\n" +
                ")";
        String insertSql = "insert into redis " +
                "select name,id " +
                "from datagen";

        tableEnvironment.executeSql(sourceSql);
        tableEnvironment.executeSql(sinkSql);
        tableEnvironment.executeSql(insertSql);
    }
}
