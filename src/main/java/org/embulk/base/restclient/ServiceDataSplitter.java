package org.embulk.base.restclient;

import org.embulk.config.TaskSource;
import org.embulk.spi.Schema;

public abstract class ServiceDataSplitter
{
    public abstract int splitToTasks(TaskSource taskSourceToHint);
    public abstract void hintPerTask(TaskSource taskSourceToHint, Schema schema, int taskIndex);
}
