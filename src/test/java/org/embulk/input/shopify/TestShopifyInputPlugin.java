/*
 * Copyright 2017 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import static org.embulk.spi.Exec.newConfigSource;
import static org.junit.Assume.assumeNotNull;

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
