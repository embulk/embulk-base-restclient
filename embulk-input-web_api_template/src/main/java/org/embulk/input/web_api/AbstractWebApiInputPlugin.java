package org.embulk.input.web_api;

import java.util.List;

import com.google.common.base.Throwables;
import org.embulk.config.TaskReport;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.input.web_api.writer.SchemaWriter;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import static org.embulk.spi.Exec.getBufferAllocator;
import static org.embulk.spi.Exec.newConfigDiff;
import static org.embulk.spi.Exec.newTaskReport;

public abstract class AbstractWebApiInputPlugin<PluginTask extends WebApiPluginTask>
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
        PluginTask task = validateInputTask(config.loadConfig(getInputTaskClass()));
        Schema schema = buildInputSchema(task);
        int taskCount = buildInputTaskCount(task); // number of run() method calls
        return resume(task.dump(), schema, taskCount, control);
    }

    protected abstract PluginTask validateInputTask(PluginTask task);

    protected abstract Class<PluginTask> getInputTaskClass();

    protected int buildInputTaskCount(PluginTask task) {
        return 1;
    }

    protected abstract Schema buildInputSchema(PluginTask task);

    protected <SchemaWrite extends SchemaWriter> Class<SchemaWrite> getSchemaWriterClass()
    {
        return (Class<SchemaWrite>) SchemaWriter.class;
    }

    protected SchemaWriter buildSchemaWriter(PluginTask task, Schema schema)
    {
        try {
            return getSchemaWriterClass().newInstance().buildColumnWriters(task, schema);
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);

        PluginTask task = taskSource.loadTask(getInputTaskClass());
        if (task.getIncremental()) {
            return buildConfigDiff(task);
        }
        else {
            return newConfigDiff();
        }
    }

    protected ConfigDiff buildConfigDiff(PluginTask task)
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
        PluginTask task = taskSource.loadTask(getInputTaskClass());
        try (PageBuilder pageBuilder = buildPageBuilder(schema, output)) {
            try {
                load(task, taskIndex, pageBuilder);
            }
            finally {
                pageBuilder.finish();
            }
        }
        return buildTaskReport(task);
    }

    protected TaskReport buildTaskReport(PluginTask task)
    {
        return newTaskReport();
    }

    protected void load(PluginTask task, int taskCount, PageBuilder to)
    {
        Schema schema = buildInputSchema(task);
        SchemaWriter schemaWriter = buildSchemaWriter(task, schema);
        fetch(task, schema, schemaWriter, taskCount, to);
    }

    protected abstract void fetch(PluginTask task, Schema schema, SchemaWriter schemaWriter, int taskCount, PageBuilder to);

    protected PageBuilder buildPageBuilder(Schema schema, PageOutput output)
    {
        return new PageBuilder(getBufferAllocator(), schema, output);
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return newConfigDiff();
    }
}
