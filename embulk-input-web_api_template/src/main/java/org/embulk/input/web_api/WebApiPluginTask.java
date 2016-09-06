package org.embulk.input.web_api;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface WebApiPluginTask
    extends Task
{
    @Config("retry_limit")
    @ConfigDefault("3")
    public int getRetryLimit();

    @Config("initial_retry_wait")
    @ConfigDefault("500")
    public int getInitialRetryWait();

    @Config("max_retry_wait")
    @ConfigDefault("1800000")
    public int getMaxRetryWait();

    @Config("stop_on_invalid_record")
    @ConfigDefault("false")
    public boolean getStopOnInvalidRecord();
}
