package org.embulk.base.restclient;

import java.util.List;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

/**
 * RestClientInputPluginBase is a base class of input plugin implementations.
 *
 * Plugin methods of this {@code RestClientInputPluginBase} are all {@code final} to avoid confusion
 * caused by inappropriate overrides. Though {@code RestClientInputPluginBaseUnsafe} is available to
 * override the plugin methods, it is really discouraged. Contact the library authors when the base
 * class does not cover use cases.
 */
public class RestClientInputPluginBase<T extends RestClientInputTaskBase>
        extends RestClientInputPluginBaseUnsafe<T>
{
    /**
     * Creates a new {@code RestClientInputPluginBase} instance.
     *
     * This constructor is designed to be called like {@code super(...);} as this class is to be inherited.
     *
     * NOTE: Specify {@code taskCount} carefully. If you specify non-{@code 1}, you need to implement
     * {@code ServiceDataIngestable} carefully to support parallel execution. Use another constructor
     * without {@code taskCount} for ordinary cases.
     */
    protected RestClientInputPluginBase(Class<T> taskClass,
                                        ConfigDiffBuildable<T> configDiffBuilder,
                                        ServiceDataIngestable<T> serviceDataIngester,
                                        ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder,
                                        TaskValidatable<T> taskValidator,
                                        int taskCount)
    {
        super(taskClass,
              configDiffBuilder,
              serviceDataIngester,
              serviceResponseMapperBuilder,
              taskValidator,
              taskCount);
    }

    /**
     * Creates a new {@code RestClientInputPluginBase} instance.
     *
     * This constructor is designed to be called like {@code super(...);} as this class is to be inherited.
     */
    protected RestClientInputPluginBase(Class<T> taskClass,
                                        ConfigDiffBuildable<T> configDiffBuilder,
                                        ServiceDataIngestable<T> serviceDataIngester,
                                        ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder,
                                        TaskValidatable<T> taskValidator)
    {
        super(taskClass,
              configDiffBuilder,
              serviceDataIngester,
              serviceResponseMapperBuilder,
              taskValidator,
              1);
    }

    /**
     * Creates a new {@code RestClientInputPluginBase} instance.
     *
     * This constructor is designed to be called like {@code super(...);} as this class is to be inherited.
     *
     * NOTE: Specify {@code taskCount} carefully. If you specify non-{@code 1}, you need to implement
     * {@code ServiceDataIngestable} carefully to support parallel execution. Use another constructor
     * without {@code taskCount} for ordinary cases.
     */
    protected RestClientInputPluginBase(Class<T> taskClass,
                                        RestClientInputPluginDelegate<T> delegate,
                                        int taskCount)
    {
        super(taskClass, delegate, delegate, delegate, delegate, taskCount);
    }

    /**
     * Creates a new {@code RestClientInputPluginBase} instance.
     *
     * This constructor is designed to be called like {@code super(...);} as this class is to be inherited.
     */
    protected RestClientInputPluginBase(Class<T> taskClass,
                                        RestClientInputPluginDelegate<T> delegate)
    {
        super(taskClass, delegate, delegate, delegate, delegate, 1);
    }

    @Override
    public final ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
    {
        return super.transaction(config, control);
    }

    @Override
    public final ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control)
    {
        return super.resume(taskSource, schema, taskCount, control);
    }

    @Override
    public final void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports)
    {
        super.cleanup(taskSource, schema, taskCount, successTaskReports);
    }

    @Override
    public final TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output)
    {
        return super.run(taskSource, schema, taskIndex, output);
    }

    @Override
    public final ConfigDiff guess(ConfigSource config)
    {
        return super.guess(config);
    }
}
