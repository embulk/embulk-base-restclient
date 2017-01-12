package org.embulk.base.restclient;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface RestClientInputTaskBase
        extends RestClientTaskBase
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

    // incremental data loading setting
    @Config("incremental")
    @ConfigDefault("true")
    public boolean getIncremental();
}
