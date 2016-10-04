package org.embulk.input.web_api;

import java.util.List;

import org.embulk.config.TaskReport;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.input.web_api.client.WebApiClient;
import org.embulk.input.web_api.schema.SchemaWrapper;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.slf4j.Logger;

import javax.ws.rs.client.Client;

import static org.embulk.spi.Exec.getBufferAllocator;
import static org.embulk.spi.Exec.newConfigDiff;
import static org.embulk.spi.Exec.newTaskReport;

public abstract class AbstractWebApiInputPlugin<TASK extends WebApiPluginTask>
        implements InputPlugin
{
    protected final Logger log;

    protected AbstractWebApiInputPlugin()
    {
        log = Exec.getLogger(AbstractWebApiInputPlugin.class);
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
    {
        TASK task = validatePluginTask(config.loadConfig(getInputTaskClass()));
        Schema schema = buildSchemaWrapper(task).newSchema();
        int taskCount = buildInputTaskCount(task); // number of run() method calls
        return resume(task.dump(), schema, taskCount, control);
    }

    protected abstract TASK validatePluginTask(TASK task);

    protected abstract Class<TASK> getInputTaskClass();

    protected int buildInputTaskCount(TASK task) {
        return 1;
    }

    protected abstract SchemaWrapper buildSchemaWrapper(TASK task);

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);

        TASK task = taskSource.loadTask(getInputTaskClass());
        if (task.getIncremental()) {
            return buildConfigDiff(task);
        }
        else {
            return newConfigDiff();
        }
    }

    protected ConfigDiff buildConfigDiff(TASK task)
    {
        return newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output)
    {
        TASK task = taskSource.loadTask(getInputTaskClass());
        try (PageBuilder pageBuilder = buildPageBuilder(schema, output)) {
            try (WebApiClient client = buildWebApiClient(task)) {
                load(task, client, buildSchemaWrapper(task), taskIndex, pageBuilder);
            }
            finally {
                pageBuilder.finish();
            }
        }
        return buildTaskReport(task);
    }

    protected abstract void load(TASK task, WebApiClient client, SchemaWrapper schemaWrapper, int taskCount, PageBuilder to);

    protected WebApiClient buildWebApiClient(TASK task)
    {
        Client client = ResteasyClientBuilder.newBuilder().build();
        return new WebApiClient.Builder().client(client).build();
    }

    protected PageBuilder buildPageBuilder(Schema schema, PageOutput output)
    {
        return new PageBuilder(getBufferAllocator(), schema, output);
    }

    protected TaskReport buildTaskReport(TASK task)
    {
        return newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return newConfigDiff();
    }
}
