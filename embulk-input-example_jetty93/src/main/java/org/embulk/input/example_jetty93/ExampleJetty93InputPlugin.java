package org.embulk.input.example_jetty93;

import org.embulk.base.restclient.RestClientInputPluginBase;

public class ExampleJetty93InputPlugin
        extends RestClientInputPluginBase<ExampleJetty93InputPluginDelegate.PluginTask>
{
    public ExampleJetty93InputPlugin()
    {
        super(ExampleJetty93InputPluginDelegate.PluginTask.class, new ExampleJetty93InputPluginDelegate());
    }
}
