package org.embulk.base.restclient;

import java.util.List;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

public class RestClientOutputPluginBaseUnsafe<T extends RestClientOutputTaskBase>
        extends RestClientPluginBase<T>
        implements OutputPlugin {
    protected RestClientOutputPluginBaseUnsafe(
            final Class<T> taskClass,
            final EmbulkDataEgestable<T> embulkDataEgester,
            final RecordBufferBuildable<T> recordBufferBuilder,
            final OutputTaskValidatable<T> outputTaskValidator,
            final ServiceRequestMapperBuildable<T> serviceRequestMapperBuilder) {
        this.taskClass = taskClass;
        this.embulkDataEgester = embulkDataEgester;
        this.recordBufferBuilder = recordBufferBuilder;
        this.outputTaskValidator = outputTaskValidator;
        this.serviceRequestMapperBuilder = serviceRequestMapperBuilder;
    }

    protected RestClientOutputPluginBaseUnsafe(final Class<T> taskClass, final RestClientOutputPluginDelegate<T> delegate) {
        this(taskClass, delegate, delegate, delegate, delegate);
    }

    @Override
    public ConfigDiff transaction(
            final ConfigSource config, final Schema schema, final int taskCount, final OutputPlugin.Control control) {
        final T task = loadConfig(config, this.taskClass);
        this.outputTaskValidator.validateOutputTask(task, schema, taskCount);
        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(
            final TaskSource taskSource, final Schema schema, final int taskCount, final OutputPlugin.Control control) {
        final T task = taskSource.loadTask(this.taskClass);
        final List<TaskReport> taskReports = control.run(taskSource);
        return this.embulkDataEgester.egestEmbulkData(task, schema, taskCount, taskReports);
    }

    @Override
    public void cleanup(
            final TaskSource taskSource, final Schema schema, final int taskCount, final List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalPageOutput open(final TaskSource taskSource, final Schema schema, final int taskIndex) {
        final T task = taskSource.loadTask(this.taskClass);
        final ServiceRequestMapper<? extends ValueLocator> serviceRequestMapper =
                this.serviceRequestMapperBuilder.buildServiceRequestMapper(task);
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
