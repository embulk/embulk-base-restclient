package org.embulk.base.restclient;

import java.util.List;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.base.restclient.request.AutoCloseableClient;
import org.embulk.base.restclient.request.RetryHelper;

public class RestClientInputPluginFragileBase<T extends RestClientInputTaskBase, U extends ValueLocator, V>
        extends RestClientPluginBase<T>
        implements InputPlugin
{
    protected RestClientInputPluginFragileBase(Class<T> taskClass,
                                               ClientCreatable<T> clientCreator,
                                               ConfigDiffBuildable<T> configDiffBuilder,
                                               ResponseReadable<V> responseReader,
                                               ServiceResponseSchemaBuildable<U> serviceResponseSchemaBuilder,
                                               PageLoadable<T,U,V> pageLoader,
                                               TaskReportBuildable<T> taskReportBuilder,
                                               TaskValidatable<T> taskValidator,
                                               int taskCount)
    {
        this.taskClass = taskClass;
        this.taskValidator = taskValidator;
        this.responseReader = responseReader;
        this.serviceResponseSchema = serviceResponseSchemaBuilder.buildServiceResponseSchema();
        this.taskReportBuilder = taskReportBuilder;
        this.configDiffBuilder = configDiffBuilder;
        this.pageLoader = pageLoader;
        this.clientCreator = clientCreator;
        this.taskCount = taskCount;
    }

    protected RestClientInputPluginFragileBase(Class<T> taskClass,
                                               RestClientInputPluginDelegate<T,U,V> delegate,
                                               int taskCount)
    {
        this(taskClass, delegate, delegate, delegate, delegate, delegate, delegate, delegate, taskCount);
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
    {
        T task = loadConfig(config, this.taskClass);
        taskValidator.validateTask(task);
        Schema schema = this.serviceResponseSchema.getEmbulkSchema();
        return resume(task.dump(), schema, this.taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        T task = taskSource.loadTask(this.taskClass);
        if (task.getIncremental()) {
            return this.configDiffBuilder.buildConfigDiff(task);
        } else {
            return Exec.newConfigDiff();
        }
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output)
    {
        T task = taskSource.loadTask(this.taskClass);
        try (PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            try (AutoCloseableClient<T> clientWrapper = new AutoCloseableClient<T>(task, this.clientCreator)) {
                RetryHelper<V> retryHelper = new RetryHelper<V>(
                    clientWrapper.getClient(),
                    this.responseReader,
                    task.getRetryLimit(),
                    task.getInitialRetryWait(),
                    task.getMaxRetryWait());
                this.pageLoader.loadPage(task,
                                         retryHelper,
                                         this.serviceResponseSchema.createSchemaWriter(),
                                         taskIndex,
                                         pageBuilder);
            }
            finally {
                pageBuilder.finish();
            }
        }
        return this.taskReportBuilder.buildTaskReport(task);
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private final Class<T> taskClass;
    private final ClientCreatable<T> clientCreator;
    private final ConfigDiffBuildable<T> configDiffBuilder;
    private final ResponseReadable<V> responseReader;
    private final PageLoadable<T,U,V> pageLoader;
    private final ServiceResponseSchema<U> serviceResponseSchema;
    private final TaskReportBuildable<T> taskReportBuilder;
    private final TaskValidatable<T> taskValidator;
    private final int taskCount;
}
