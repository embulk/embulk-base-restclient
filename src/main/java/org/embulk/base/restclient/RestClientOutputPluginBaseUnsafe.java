package org.embulk.base.restclient;

import java.util.List;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import org.embulk.base.restclient.record.ValueLocator;

public class RestClientOutputPluginBaseUnsafe<T extends RestClientOutputTaskBase>
        extends RestClientPluginBase<T>
        implements OutputPlugin
{
    protected RestClientOutputPluginBaseUnsafe(Class<T> taskClass,
                                               EmbulkDataEgestable<T> embulkDataEgester,
                                               RecordBufferBuildable<T> recordBufferBuilder,
                                               OutputTaskValidatable<T> outputTaskValidator,
                                               ServiceRequestMapperBuildable<T> serviceRequestMapperBuilder)
    {
        this.taskClass = taskClass;
        this.embulkDataEgester = embulkDataEgester;
        this.recordBufferBuilder = recordBufferBuilder;
        this.outputTaskValidator = outputTaskValidator;
        this.serviceRequestMapperBuilder = serviceRequestMapperBuilder;
    }

    protected RestClientOutputPluginBaseUnsafe(Class<T> taskClass,
                                               RestClientOutputPluginDelegate<T> delegate)
    {
        this(taskClass, delegate, delegate, delegate, delegate);
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control)
    {
        T task = loadConfig(config, this.taskClass);
        this.outputTaskValidator.validateOutputTask(task, schema, taskCount);
        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control)
    {
        T task = taskSource.loadTask(this.taskClass);
        List<TaskReport> taskReports = control.run(taskSource);
        return this.embulkDataEgester.egestEmbulkData(task, schema, taskCount, taskReports);
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        T task = taskSource.loadTask(this.taskClass);
        ServiceRequestMapper<? extends ValueLocator> serviceRequestMapper =
            this.serviceRequestMapperBuilder.buildServiceRequestMapper(task, schema);
        return new RestClientPageOutput<T>(this.taskClass,
                                           task,
                                           serviceRequestMapper.createRecordExporter(),
                                           this.recordBufferBuilder.buildRecordBuffer(task, schema, taskIndex),
                                           schema,
                                           taskIndex);
    }

    private final Class<T> taskClass;
    private final EmbulkDataEgestable<T> embulkDataEgester;
    private final RecordBufferBuildable<T> recordBufferBuilder;
    private final OutputTaskValidatable<T> outputTaskValidator;
    private final ServiceRequestMapperBuildable<T> serviceRequestMapperBuilder;
}
