package org.embulk.input.shopify;

import org.embulk.base.restclient.RestClientInputPluginBase;
import org.embulk.base.restclient.record.JacksonValueLocator;

public class ShopifyInputPlugin
        extends RestClientInputPluginBase<ShopifyInputPluginDelegate.PluginTask, JacksonValueLocator>
{
    public ShopifyInputPlugin()
    {
        super(ShopifyInputPluginDelegate.PluginTask.class, new ShopifyInputPluginDelegate(), 1);
    }
}
