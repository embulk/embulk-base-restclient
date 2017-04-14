package org.embulk.base.restclient;

import org.embulk.config.TaskSource;

public class DefaultServiceDataDispatcherBuilder<T extends RestClientInputTaskBase>
        implements ServiceDataDispatcherBuildable<T>
{
    public ServiceDataDispatcher buildServiceDataDispatcher(T task)
    {
        return new ServiceDataDispatcher() {
            @Override
            public int splitToTasks(TaskSource taskSourceToHint)
            {
                return 1;
            }

            @Override
            public void hintPerTask(int taskIndex, TaskSource taskSourceToHint)
            {
            }
        };
    }
}
