package org.embulk.base.restclient;

import java.util.List;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

import org.embulk.base.restclient.record.ValueLocator;

public class RestClientInputPluginBase<T extends RestClientInputTaskBase, U extends ValueLocator, V>
        extends RestClientInputPluginFragileBase<T, U, V>
{
    protected RestClientInputPluginBase(Class<T> taskClass,
                                        ClientCreatable<T> clientCreator,
                                        ConfigDiffBuildable<T> configDiffBuilder,
                                        ResponseReadable<V> responseReader,
                                        ServiceResponseSchemaBuildable<U> serviceResponseSchemaBuilder,
                                        PageLoadable<T,U,V> pageLoader,
                                        TaskReportBuildable<T> taskReportBuilder,
                                        TaskValidatable<T> taskValidator,
                                        int taskCount)
    {
        super(taskClass,
              clientCreator,
              configDiffBuilder,
              responseReader,
              serviceResponseSchemaBuilder,
              pageLoader,
              taskReportBuilder,
              taskValidator,
              taskCount);
    }

    protected RestClientInputPluginBase(Class<T> taskClass,
                                        RestClientInputPluginDelegate<T, U, V> delegate,
                                        int taskCount)
    {
        super(taskClass, delegate, delegate, delegate, delegate, delegate, delegate, delegate, taskCount);
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
    public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output)
    {
        return super.run(taskSource, schema, taskIndex, output);
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return super.guess(config);
    }
}
