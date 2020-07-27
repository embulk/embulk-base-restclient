package org.embulk.base.restclient;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;

public abstract class RestClientPluginBase<T extends RestClientTaskBase> {
    protected final T loadConfig(final ConfigSource config, final Class<T> taskClass) {
        return config.loadConfig(taskClass);
    }

    protected final T loadTask(final TaskSource taskSource, final Class<T> taskClass) {
        return taskSource.loadTask(taskClass);
    }
}
