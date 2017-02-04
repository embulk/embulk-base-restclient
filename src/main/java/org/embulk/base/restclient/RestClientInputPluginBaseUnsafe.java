package org.embulk.base.restclient;

import java.util.List;

import com.google.common.base.Preconditions;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

import org.embulk.base.restclient.request.AutoCloseableClient;
import org.embulk.base.restclient.request.RetryHelper;

/**
 * RestClientInputPluginBaseUnsafe is an "unsafe" base class of input plugin implementations.
 *
 * Do not use this {@code RestClientInputPluginBaseUnsafe} directly unless you really need this.
 * For almost all cases, inherit {@code RestClientInputPluginBase} instead.
 *
 * This {@code RestClientInputPluginBaseUnsafe} is here to allow overriding the plugin methods
 * such as {@code transaction} and {@code resume}. Overriding them can cause unexpected behavior.
 * Plugins using this {@code RestClientInputPluginBaseUnsafe} may have difficulties to catch up
 * with updates of this library.
 *
 * Use {@code RestClientInputPluginBase} instead of {@code RestClientInputPluginBaseUnsafe}.
 * Plugin methods of {@code RestClientInputPluginBase} are all {@code final} not to be overridden.
 */
public class RestClientInputPluginBaseUnsafe<T extends RestClientInputTaskBase>
        extends RestClientPluginBase<T>
        implements InputPlugin
{
    protected RestClientInputPluginBaseUnsafe(Class<T> taskClass,
                                              ClientCreatable<T> clientCreator,
                                              ConfigDiffBuildable<T> configDiffBuilder,
                                              ServiceDataIngestable<T> serviceDataIngester,
                                              ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder,
                                              TaskValidatable<T> taskValidator,
                                              int taskCount)
    {
        this.taskClass = taskClass;
        this.taskValidator = taskValidator;
        this.serviceResponseMapperBuilder = serviceResponseMapperBuilder;
        this.configDiffBuilder = configDiffBuilder;
        this.serviceDataIngester = serviceDataIngester;
        this.clientCreator = clientCreator;
        this.taskCount = taskCount;
    }

    protected RestClientInputPluginBaseUnsafe(Class<T> taskClass,
                                              RestClientInputPluginDelegate<T> delegate,
                                              int taskCount)
    {
        this(taskClass, delegate, delegate, delegate, delegate, delegate, taskCount);
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
    {
        T task = loadConfig(config, this.taskClass);
        taskValidator.validateTask(task);
        Schema schema = this.serviceResponseMapperBuilder.buildServiceResponseMapper(task).getEmbulkSchema();
        return resume(task.dump(), schema, this.taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control)
    {
        T task = taskSource.loadTask(this.taskClass);
        List<TaskReport> taskReports = control.run(taskSource, schema, taskCount);
        if (task.getIncremental()) {
            return this.configDiffBuilder.buildConfigDiff(task, schema, taskCount, taskReports);
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
        ServiceResponseMapper serviceResponseMapper =
            this.serviceResponseMapperBuilder.buildServiceResponseMapper(task);

        try (PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            try (AutoCloseableClient<T> clientWrapper = new AutoCloseableClient<T>(task, this.clientCreator)) {
                RetryHelper retryHelper = new RetryHelper(
                    clientWrapper.getClient(),
                    task.getRetryLimit(),
                    task.getInitialRetryWait(),
                    task.getMaxRetryWait());
                return Preconditions.checkNotNull(
                    this.serviceDataIngester.ingestServiceData(
                        task,
                        retryHelper,
                        serviceResponseMapper.createRecordImporter(),
                        taskIndex,
                        pageBuilder));
            }
            finally {
                pageBuilder.finish();
            }
        }
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private final Class<T> taskClass;
    private final ClientCreatable<T> clientCreator;
    private final ConfigDiffBuildable<T> configDiffBuilder;
    private final ServiceDataIngestable<T> serviceDataIngester;
    private final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder;
    private final TaskValidatable<T> taskValidator;
    private final int taskCount;
}
