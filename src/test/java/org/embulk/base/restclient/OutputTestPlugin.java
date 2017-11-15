package org.embulk.base.restclient;

class OutputTestPlugin
        extends RestClientOutputPluginBase<OutputTestPluginDelegate.PluginTask>
{
    OutputTestPlugin()
    {
        super(OutputTestPluginDelegate.PluginTask.class, new OutputTestPluginDelegate(true));
    }
}