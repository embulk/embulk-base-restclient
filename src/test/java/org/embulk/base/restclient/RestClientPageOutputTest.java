package org.embulk.base.restclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.embulk.base.restclient.OutputTestPluginDelegate.PluginTask;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RestClientPageOutputTest {

    @BeforeClass
    public static void initializeConstant()
    {
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private OutputTestUtils utils;
    private OutputTestPlugin plugin;

    @Before
    public void createResources() throws Exception
    {
        utils = new OutputTestUtils();
        utils.initializeConstant();
//        PluginTask task = utils.configJSON().loadConfig(PluginTask.class);

        plugin = new OutputTestPlugin();
    }

    @Test
    public void testSomething()
    {

    }

    @Test
    public void testOutputWithNullValues() throws Exception
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);

        // id, long, timestamp, boolean, double, string
        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, 1L, null, null, null, null, null);
        assertThat(pages.size(), is(1));
        for (Page page : pages) {
            output.add(page);
        }

        output.finish();
        output.commit();

        ArrayList<OutputTestRecordBuffer> buffers = OutputTestPluginDelegate.getBuffers();

        String res = buffers.get(0).toString();

        assertThat(res, is("[{\"id\":1,\"long\":null,\"timestamp\":null,\"boolean\":null,\"double\":null,\"string\":null}]"));
    }

    @Test
    public void testOutputWithRegularValues() throws Exception
    {
        ConfigSource config = utils.configJSON();
        Schema schema = utils.JSONSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);

        // id, long, timestamp, boolean, double, string
        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, 2L, 42L, Timestamp.ofEpochSecond(1509738161), true, 123.45, "embulk");
        assertThat(pages.size(), is(1));
        for (Page page : pages) {
            output.add(page);
        }

        output.finish();
        output.commit();

        ArrayList<OutputTestRecordBuffer> buffers = OutputTestPluginDelegate.getBuffers();

        String res = buffers.get(0).toString();

        assertThat(res, is("[{\"id\":2,\"long\":42,\"timestamp\":\"2017-11-03T19:42:41.000+0000\",\"boolean\":true,\"double\":123.45,\"string\":\"embulk\"}]"));
    }
}