package org.embulk.input.web_api;

import java.util.List;
import org.embulk.config.TaskReport;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

import static org.embulk.spi.Exec.getBufferAllocator;
import static org.embulk.spi.Exec.newConfigDiff;
import static org.embulk.spi.Exec.newTaskReport;

public abstract class AbstractWebApiInputPlugin<PluginTask extends WebApiPluginTask>
        implements InputPlugin
{
    @Override
    public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control)
    {
        PluginTask task = validate(config.loadConfig(getTaskClass()));
        Schema schema = buildSchema(task);
        int taskCount = buildTaskCount(task); // number of run() method calls
        return resume(task.dump(), schema, taskCount, control);
    }

    protected abstract PluginTask validate(PluginTask task);

    protected abstract Class<PluginTask> getTaskClass();

    protected abstract Schema buildSchema(PluginTask task);

    protected int buildTaskCount(PluginTask task) {
        return 1;
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(getTaskClass());
        try (PageBuilder pageBuilder = newPageBuilder(schema, output)) {
            try {
                load(task, taskIndex, pageBuilder);
            }
            finally {
                pageBuilder.finish();
            }
        }
        return newTaskReport();
    }

    protected abstract void load(PluginTask task, int taskCount, PageBuilder to);

    private PageBuilder newPageBuilder(Schema schema, PageOutput output)
    {
        return new PageBuilder(getBufferAllocator(), schema, output);
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return newConfigDiff();
    }
}
