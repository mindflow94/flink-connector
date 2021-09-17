package org.apache.flink.streaming.connectors.redis.common.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.Util;

import java.io.Serializable;

/**
 * Base class for Flink Redis configuration.
 */
public abstract class FlinkJedisConfigBase implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final int maxTotal;
    protected final int maxIdle;
    protected final int minIdle;
    protected final int connectionTimeout;
    protected final String password;

    protected final boolean testOnBorrow;
    protected final boolean testOnReturn;
    protected final boolean testWhileIdle;

    protected FlinkJedisConfigBase(int connectionTimeout, int maxTotal, int maxIdle, int minIdle, String password, boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle) {

        Util.checkArgument(connectionTimeout >= 0, "connection timeout can not be negative");
        Util.checkArgument(maxTotal >= 0, "maxTotal value can not be negative");
        Util.checkArgument(maxIdle >= 0, "maxIdle value can not be negative");
        Util.checkArgument(minIdle >= 0, "minIdle value can not be negative");

        this.connectionTimeout = connectionTimeout;
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        this.minIdle = minIdle;
        this.testOnBorrow = testOnBorrow;
        this.testOnReturn = testOnReturn;
        this.testWhileIdle = testWhileIdle;
        this.password = password;
    }

    /**
     * Returns timeout.
     *
     * @return connection timeout
     */
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Get the value for the {@code maxTotal} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @return The current setting of {@code maxTotal} for this
     * configuration instance
     * @see GenericObjectPoolConfig#getMaxTotal()
     */
    public int getMaxTotal() {
        return maxTotal;
    }

    /**
     * Get the value for the {@code maxIdle} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @return The current setting of {@code maxIdle} for this
     * configuration instance
     * @see GenericObjectPoolConfig#getMaxIdle()
     */
    public int getMaxIdle() {
        return maxIdle;
    }

    /**
     * Get the value for the {@code minIdle} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @return The current setting of {@code minIdle} for this
     * configuration instance
     * @see GenericObjectPoolConfig#getMinIdle()
     */
    public int getMinIdle() {
        return minIdle;
    }

    /**
     * Returns password.
     *
     * @return password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Get the value for the {@code testOnBorrow} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @return The current setting of {@code testOnBorrow} for this
     * configuration instance
     * @see GenericObjectPoolConfig#getTestOnBorrow()
     */
    public boolean getTestOnBorrow() {
        return testOnBorrow;
    }

    /**
     * Get the value for the {@code testOnReturn} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @return The current setting of {@code testOnReturn} for this
     * configuration instance
     * @see GenericObjectPoolConfig#getTestOnReturn()
     */
    public boolean getTestOnReturn() {
        return testOnReturn;
    }

    /**
     * Get the value for the {@code testWhileIdle} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @return The current setting of {@code testWhileIdle} for this
     * configuration instance
     * @see GenericObjectPoolConfig#getTestWhileIdle()
     */
    public boolean getTestWhileIdle() {
        return testWhileIdle;
    }
}
