package org.embulk.base.restclient;

class NoNullsOutputTestPlugin
        extends RestClientOutputPluginBase<OutputTestPluginDelegate.PluginTask>
{
    NoNullsOutputTestPlugin()
    {
        super(OutputTestPluginDelegate.PluginTask.class, new OutputTestPluginDelegate(false));
    }
}