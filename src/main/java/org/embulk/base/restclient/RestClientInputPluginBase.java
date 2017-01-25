package org.embulk.base.restclient;

import java.util.List;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

public class RestClientInputPluginBase<T extends RestClientInputTaskBase>
        extends RestClientInputPluginFragileBase<T>
{
    protected RestClientInputPluginBase(Class<T> taskClass,
                                        ClientCreatable<T> clientCreator,
                                        ConfigDiffBuildable<T> configDiffBuilder,
                                        ServiceResponseSchemaBuildable serviceResponseSchemaBuilder,
                                        PageLoadable<T> pageLoader,
                                        TaskReportBuildable<T> taskReportBuilder,
                                        TaskValidatable<T> taskValidator,
                                        int taskCount)
    {
        super(taskClass,
              clientCreator,
              configDiffBuilder,
              serviceResponseSchemaBuilder,
              pageLoader,
              taskReportBuilder,
              taskValidator,
              taskCount);
    }

    protected RestClientInputPluginBase(Class<T> taskClass,
                                        RestClientInputPluginDelegate<T> delegate,
                                        int taskCount)
    {
        super(taskClass, delegate, delegate, delegate, delegate, delegate, delegate, taskCount);
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
