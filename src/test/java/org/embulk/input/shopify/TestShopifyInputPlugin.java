package org.embulk.input.shopify;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;

import static org.junit.Assume.assumeNotNull;

import static org.embulk.spi.Exec.newConfigSource;

public class TestShopifyInputPlugin
{
    private static String shopifyApiKey;
    private static String shopifyPassword;
    private static String shopifyStoreName;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;
    private ShopifyInputPlugin plugin;
    private MockPageOutput output;


    @BeforeClass
    public static void initializeConstant()
    {
        shopifyApiKey = System.getenv("SHOPIFY_APIKEY");
        shopifyPassword = System.getenv("SHOPIFY_PASSWORD");
        shopifyStoreName = System.getenv("SHOPIFY_STORE_NAME");
        assumeNotNull(shopifyApiKey, shopifyPassword, shopifyStoreName);
    }

    @Before
    public void createResources() throws Exception
    {
        config = config();
        plugin = new ShopifyInputPlugin();
        output = new MockPageOutput();
    }

    @Test
    public void simpleTest()
            throws Exception
    {
        ConfigSource config = this.config.deepCopy()
                .set("from_date", "2016-10-16");
        plugin.transaction(config, new Control());

        // TODO compare expected and actual
    }

    private class Control
            implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(plugin.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }

    private ConfigSource config()
    {
        return newConfigSource()
                .set("apikey", shopifyApiKey)
                .set("password", shopifyPassword)
                .set("store_name", shopifyStoreName);
    }
}
