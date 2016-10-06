package org.embulk.input.web_api;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.util.web_api.AbstractWebApiInputPlugin;
import org.embulk.util.web_api.WebApiPluginTask;
import org.embulk.util.web_api.client.AbstractRetryableWebApiCall;
import org.embulk.util.web_api.client.WebApiClient;
import org.embulk.util.web_api.json.JsonParser;
import org.embulk.util.web_api.schema.SchemaWrapper;
import org.embulk.util.web_api.writer.SchemaWriter;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Types;

import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;

import java.io.IOException;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Locale.ENGLISH;
import static org.embulk.spi.Exec.newConfigDiff;
import static org.embulk.spi.Exec.newConfigSource;

public class WebApiInputPlugin
        extends AbstractWebApiInputPlugin<WebApiInputPlugin.PluginTask>
{
    public interface PluginTask
            extends WebApiPluginTask
    {
        // Define additional config options

        @Config("apikey")
        public String getApiKey();
    }

    @Override
    protected PluginTask validatePluginTask(PluginTask task)
    {
        // Define validation for config options

        if (isNullOrEmpty(task.getApiKey())) {
            throw new ConfigException("'apikey' must not be null or empty string.");
        }

        return task;
    }

    @Override
    protected Class<PluginTask> getInputTaskClass()
    {
        return PluginTask.class;
    }

    @Override
    protected SchemaWrapper buildSchemaWrapper(PluginTask task)
    {
        ConfigSource timestampConfig = newConfigSource().set("format", "%Y-%m-%d %H:%M:%S");

        return new SchemaWrapper.Builder()
                .add("id", Types.LONG)
                .add("name", Types.STRING)
                .add("created_at", Types.TIMESTAMP, timestampConfig)
                .add("updated_at", Types.TIMESTAMP, timestampConfig)
                .build();
    }

    @Override
    protected ConfigDiff buildConfigDiff(PluginTask task)
    {
        // do nothing
        return newConfigDiff();
    }

    private static final int PAGE_LIMIT = 250;

    @Override
    protected void load(PluginTask task, WebApiClient client, SchemaWrapper schemaWrapper, int taskCount, PageBuilder to)
    {
        SchemaWriter schemaWriter = schemaWrapper.newSchemaWriter();
        try {
            int pageIndex = 1;

            while (true) {
                int size = 0;

                String content = fetchFromWebApi(task, client, pageIndex);
                JsonNode json = new JsonParser().parseJsonArray(content);
                if (JsonParser.isNotJsonArray(json) && (size = json.size()) > 0) {
                    for (int i = 0; i < size; i++) {
                        final JsonNode record = json.get(i);
                        if (JsonParser.isNotJsonObject(record)) {
                            continue;
                        }

                        try {
                            schemaWriter.write(record, to);
                            to.addRecord();
                        }
                        catch (Exception e) {
                            log.warn(String.format(ENGLISH, "Skipped json: %s", record.toString()), e);
                        }
                    }
                }
                else {
                    log.info("No data available");
                }

                if (size < PAGE_LIMIT) {
                    break;
                }

                pageIndex = pageIndex + 1;
            }

        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private String fetchFromWebApi(final PluginTask task, final WebApiClient client, final int pageIndex)
            throws IOException
    {
        return client.fetchWithRetry(task, new AbstractRetryableWebApiCall<PluginTask, String>() {
            @Override
            public Response request(PluginTask pluginTask)
            {
                // Here we call some APIs and fetch exported data.
                // For example, ...

                final String url = "https://my_endpoint/path";
                return client.getClient()
                        .target(url)
                        .queryParam("page", pageIndex)
                        .queryParam("limit", PAGE_LIMIT)
                        .request()
                        .header("AUTHORIZATION", "Basic " + DatatypeConverter.printBase64Binary(task.getApiKey().getBytes()))
                        .get();
            }

            @Override
            public String readResponse(Response response)
            {
                return response.readEntity(String.class);
            }
        });
    }
}
