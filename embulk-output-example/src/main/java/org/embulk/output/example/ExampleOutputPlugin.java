package org.embulk.output.example;

import org.embulk.base.restclient.RestClientOutputPluginBase;

public class ExampleOutputPlugin
        extends RestClientOutputPluginBase<ExampleOutputPluginDelegate.PluginTask>
{
    public ExampleOutputPlugin()
    {
        super(ExampleOutputPluginDelegate.PluginTask.class, new ExampleOutputPluginDelegate());
    }
}
