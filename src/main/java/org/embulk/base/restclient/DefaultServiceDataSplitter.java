package org.embulk.base.restclient;

import org.embulk.config.TaskSource;
import org.embulk.spi.Schema;

public class DefaultServiceDataSplitter
        extends ServiceDataSplitter
{
    @Override
    public int splitToTasks(TaskSource taskSourceToHint)
    {
        return 1;
    }

    @Override
    public void hintPerTask(TaskSource taskSourceToHint, Schema schema, int taskIndex)
    {
    }
}
