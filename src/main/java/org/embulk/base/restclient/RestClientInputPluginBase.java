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

import org.slf4j.Logger;

import org.embulk.base.restclient.record.JacksonValueLocator;
import org.embulk.base.restclient.request.AutoCloseableClient;
import org.embulk.base.restclient.request.RetryHelper;
import org.embulk.base.restclient.writer.SchemaWriter;

import static org.embulk.spi.Exec.getBufferAllocator;
import static org.embulk.spi.Exec.newConfigDiff;
import static org.embulk.spi.Exec.newTaskReport;

public abstract class RestClientInputPluginBase<T extends RestClientInputTaskBase>
        extends RestClientPluginBase<T>
        implements InputPlugin
{
    protected final Logger log;

    protected RestClientInputPluginBase(ClientCreatable<T> clientCreator,
                                        ServiceResponseSchemaBuildable<JacksonValueLocator> serviceResponseSchemaBuilder)
    {
        log = Exec.getLogger(RestClientInputPluginBase.class);
        this.clientCreator = clientCreator;
        this.serviceResponseSchema = serviceResponseSchemaBuilder.buildServiceResponseSchema();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
    {
        T task = validatePluginTask(config.loadConfig(getInputTaskClass()));
        Schema schema = this.serviceResponseSchema.getEmbulkSchema();
        int taskCount = buildInputTaskCount(task); // number of run() method calls
        return resume(task.dump(), schema, taskCount, control);
    }

    protected abstract T validatePluginTask(T task);

    protected abstract Class<T> getInputTaskClass();

    protected int buildInputTaskCount(T task)
    {
        return 1;
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);

        T task = taskSource.loadTask(getInputTaskClass());
        if (task.getIncremental()) {
            return buildConfigDiff(task);
        }
        else {
            return newConfigDiff();
        }
    }

    protected ConfigDiff buildConfigDiff(T task)
    {
        return newConfigDiff(); // for incremental data loading
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output)
    {
        T task = taskSource.loadTask(getInputTaskClass());
        try (PageBuilder pageBuilder = buildPageBuilder(schema, output)) {
            try (AutoCloseableClient<T> clientWrapper = new AutoCloseableClient<T>(task, this.clientCreator)) {
                RetryHelper retryHelper = new RetryHelper(
                        clientWrapper.getClient(),
                        task.getRetryLimit(),
                        task.getInitialRetryWait(),
                        task.getMaxRetryWait());
                load(task, retryHelper, this.serviceResponseSchema.createSchemaWriter(), taskIndex, pageBuilder);
            }
            finally {
                pageBuilder.finish();
            }
        }
        return buildTaskReport(task);
    }

    protected abstract void load(T task, RetryHelper client, SchemaWriter<JacksonValueLocator> schemaWriter, int taskCount, PageBuilder to);

    protected PageBuilder buildPageBuilder(Schema schema, PageOutput output)
    {
        return new PageBuilder(getBufferAllocator(), schema, output);
    }

    protected TaskReport buildTaskReport(T task)
    {
        return newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return newConfigDiff();
    }
    
    private final ClientCreatable<T> clientCreator;
    private final ServiceResponseSchema<JacksonValueLocator> serviceResponseSchema;
}
