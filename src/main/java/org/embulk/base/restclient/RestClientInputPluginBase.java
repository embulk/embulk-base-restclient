package org.embulk.base.restclient;

import java.util.List;

import javax.ws.rs.client.Client;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;

import org.slf4j.Logger;

import org.embulk.base.restclient.request.RetryHelper;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.embulk.spi.Exec.getBufferAllocator;
import static org.embulk.spi.Exec.newConfigDiff;
import static org.embulk.spi.Exec.newTaskReport;

public abstract class RestClientInputPluginBase<T extends RestClientInputTaskBase>
        extends RestClientPluginBase<T>
        implements InputPlugin
{
    protected final Logger log;

    protected RestClientInputPluginBase()
    {
        log = Exec.getLogger(RestClientInputPluginBase.class);
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
    {
        T task = validatePluginTask(config.loadConfig(getInputTaskClass()));
        Schema schema = buildSchemaWriterFactory(task).newSchema();
        int taskCount = buildInputTaskCount(task); // number of run() method calls
        return resume(task.dump(), schema, taskCount, control);
    }

    protected abstract T validatePluginTask(T task);

    protected abstract Class<T> getInputTaskClass();

    protected int buildInputTaskCount(T task)
    {
        return 1;
    }

    protected abstract JacksonServiceResponseSchema buildSchemaWriterFactory(T task);

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
            try (RetryHelper client = buildWebApiClient(task)) {
                load(task, client, buildSchemaWriterFactory(task), taskIndex, pageBuilder);
            }
            finally {
                pageBuilder.finish();
            }
        }
        return buildTaskReport(task);
    }

    protected abstract void load(T task, RetryHelper client, JacksonServiceResponseSchema schemaWriterFactory, int taskCount, PageBuilder to);

    protected RetryHelper buildWebApiClient(T task)
    {
        Client client = ((ResteasyClientBuilder) ResteasyClientBuilder.newBuilder())
                .connectionCheckoutTimeout(task.getConnectionCheckoutTimeout(), MILLISECONDS)
                .establishConnectionTimeout(task.getEstablishCheckoutTimeout(), MILLISECONDS)
                .socketTimeout(task.getSocketTimeout(), MILLISECONDS)
                .connectionPoolSize(task.getConnectionPoolSize())
                .build();
        return RetryHelper.builder().client(client).build(task);
    }

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
}
