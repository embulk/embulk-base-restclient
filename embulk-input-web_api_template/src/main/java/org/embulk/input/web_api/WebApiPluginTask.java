package org.embulk.input.web_api;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.spi.time.TimestampParser;

public interface WebApiPluginTask
    extends Task, TimestampParser.Task, TimestampParser.TimestampColumnOption
{
    @Config("retry_limit")
    @ConfigDefault("7")
    public int getRetryLimit();

    @Config("initial_retry_wait")
    @ConfigDefault("1000")
    public int getInitialRetryWait();

    @Config("max_retry_wait")
    @ConfigDefault("60000")
    public int getMaxRetryWait();

    @Config("stop_on_invalid_record")
    @ConfigDefault("false")
    public boolean getStopOnInvalidRecord();

    @Config("incremental")
    @ConfigDefault("true")
    public boolean getIncremental();
}
