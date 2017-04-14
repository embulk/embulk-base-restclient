package org.embulk.base.restclient;

import org.embulk.config.TaskSource;

public abstract class ServiceDataDispatcher
{
    public abstract int splitToTasks(TaskSource taskSourceToHint);
    public abstract void hintPerTask(int taskIndex, TaskSource taskSourceToHint);
}
