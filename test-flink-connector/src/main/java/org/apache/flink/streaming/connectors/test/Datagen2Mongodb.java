package org.apache.flink.streaming.connectors.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class Datagen2Mongodb {

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
}
