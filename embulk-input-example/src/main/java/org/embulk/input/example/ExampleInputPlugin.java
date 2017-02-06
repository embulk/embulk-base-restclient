package org.embulk.input.example;

import org.embulk.base.restclient.RestClientInputPluginBase;

public class ExampleInputPlugin
        extends RestClientInputPluginBase<ExampleInputPluginDelegate.PluginTask>
{
    public ExampleInputPlugin()
    {
        super(ExampleInputPluginDelegate.PluginTask.class, new ExampleInputPluginDelegate());
    }
}
