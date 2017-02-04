package org.embulk.input.shopify;

import org.embulk.base.restclient.RestClientInputPluginBase;

public class ShopifyInputPlugin
        extends RestClientInputPluginBase<ShopifyInputPluginDelegate.PluginTask>
{
    public ShopifyInputPlugin()
    {
        super(ShopifyInputPluginDelegate.PluginTask.class, new ShopifyInputPluginDelegate());
    }
}
