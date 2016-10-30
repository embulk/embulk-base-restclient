package org.embulk.util.web_api;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface WebApiPluginTask
    extends Task
{
    // client retry setting
    @Config("retry_limit")
    @ConfigDefault("7")
    public int getRetryLimit();

    @Config("initial_retry_wait")
    @ConfigDefault("1000")
    public int getInitialRetryWait();

    @Config("max_retry_wait")
    @ConfigDefault("60000")
    public int getMaxRetryWait();

    // client timeout and connection setting
    @Config("connection_checkout_timeout")
    @ConfigDefault("30000")
    public long getConnectionCheckoutTimeout(); // millis

    @Config("establish_connection_timeout")
    @ConfigDefault("30000")
    public long getEstablishCheckoutTimeout(); // millis

    @Config("socket_timeout")
    @ConfigDefault("60000")
    public long getSocketTimeout(); // millis

    @Config("connection_pool_size")
    @ConfigDefault("8")
    public int getConnectionPoolSize();

    @Config("stop_on_invalid_record")
    @ConfigDefault("false")
    public boolean getStopOnInvalidRecord();

    // incremental data loading setting
    @Config("incremental")
    @ConfigDefault("true")
    public boolean getIncremental();

    @Config("from_date")
    public String getFromDate();

    @Config("fetch_days")
    @ConfigDefault("1")
    public int getFetchDays();
}
