package org.apache.flink.streaming.connectors.mongodb.sink;

import org.apache.flink.streaming.connectors.mongodb.MongodbConf;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class MongodbSinkConf extends MongodbConf {
    private final int batchSize;

    public MongodbSinkConf(String database, String collection, String uri, int maxConnectionIdleTime, int batchSize) {
        super(database, collection, uri, maxConnectionIdleTime);
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    @Override
    public String toString() {
        return "MongodbSinkConf{" + super.toString() + "batchSize=" + this.batchSize + '}';
    }
}
