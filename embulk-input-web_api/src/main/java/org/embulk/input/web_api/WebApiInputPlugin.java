package org.embulk.input.web_api;

import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;

public class WebApiInputPlugin
        extends AbstractWebApiInputPlugin<WebApiInputPlugin.PluginTask>
{
    public interface PluginTask
            extends WebApiPluginTask
    {
        // TODO
    }

    @Override
    protected PluginTask validate(PluginTask task)
    {
        return task;
    }

    @Override
    protected Class<PluginTask> getTaskClass()
    {
        return PluginTask.class;
    }

    @Override
    protected Schema buildSchema(PluginTask task)
    {
        throw new UnsupportedOperationException("should implement");
    }

    @Override
    protected void load(PluginTask task, int taskCount, PageBuilder to)
    {
        throw new UnsupportedOperationException("should implement");
    }
}
