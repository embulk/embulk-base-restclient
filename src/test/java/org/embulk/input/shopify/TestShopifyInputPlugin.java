package org.embulk.input.shopify;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.embulk.spi.Exec.newConfigSource;
import static org.junit.Assume.assumeNotNull;

public class TestShopifyInputPlugin
{
    private static String SHOPIFY_APIKEY;
    private static String SHOPIFY_PASSWORD;
    private static String SHOPIFY_STORE_NAME;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;
    private ShopifyInputPlugin plugin;
    private MockPageOutput output;


    @BeforeClass
    public static void initializeConstant()
    {
        SHOPIFY_APIKEY = System.getenv("SHOPIFY_APIKEY");
        SHOPIFY_PASSWORD = System.getenv("SHOPIFY_PASSWORD");
        SHOPIFY_STORE_NAME = System.getenv("SHOPIFY_STORE_NAME");
        assumeNotNull(SHOPIFY_APIKEY, SHOPIFY_PASSWORD, SHOPIFY_STORE_NAME);
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
                .set("apikey", SHOPIFY_APIKEY)
                .set("password", SHOPIFY_PASSWORD)
                .set("store_name", SHOPIFY_STORE_NAME);
    }
}
