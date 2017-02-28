package org.embulk.input.example_jetty93;

import java.util.List;
import java.util.Locale;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.common.io.CharStreams;

import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.client.api.ContentResponse;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import org.embulk.base.restclient.RestClientInputPluginDelegate;
import org.embulk.base.restclient.RestClientInputTaskBase;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.jackson.StringJsonParser;
import org.embulk.base.restclient.record.RecordImporter;

import org.embulk.util.retryhelper.jetty93.DefaultJetty93ClientCreator;
import org.embulk.util.retryhelper.jetty93.Jetty93RetryHelper;
import org.embulk.util.retryhelper.jetty93.Jetty93SingleRequester;
import org.embulk.util.retryhelper.jetty93.StringJetty93ResponseEntityReader;

import org.slf4j.Logger;

public class ExampleJetty93InputPluginDelegate
        implements RestClientInputPluginDelegate<ExampleJetty93InputPluginDelegate.PluginTask>
{
    public interface PluginTask
            extends RestClientInputTaskBase
    {
        @Config("maximum_retries")
        @ConfigDefault("7")
        public int getMaximumRetries();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("1000")
        public int getInitialRetryIntervalMillis();

        @Config("maximum_retry_interval_millis")
        @ConfigDefault("60000")
        public int getMaximumRetryIntervalMillis();
    }

    private final StringJsonParser jsonParser = new StringJsonParser();

    @Override  // Overridden from |TaskValidatable|
    public void validateTask(PluginTask task)
    {
    }

    @Override  // Overridden from |ServiceResponseMapperBuildable|
    public JacksonServiceResponseMapper buildServiceResponseMapper(PluginTask task)
    {
        return JacksonServiceResponseMapper.builder()
            .add("id", Types.LONG)
            .build();
    }

    @Override  // Overridden from |ConfigDiffBuildable|
    public ConfigDiff buildConfigDiff(PluginTask task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        // should implement for incremental data loading
        // consider |incremental| config here
        return Exec.newConfigDiff();
    }

    @Override  // Overridden from |ServiceDataIngestable|
    public TaskReport ingestServiceData(final PluginTask task,
                                        RecordImporter recordImporter,
                                        int taskIndex,
                                        PageBuilder pageBuilder)
    {
        try (Jetty93RetryHelper retryHelper = new Jetty93RetryHelper(
                 task.getMaximumRetries(),
                 task.getInitialRetryIntervalMillis(),
                 task.getMaximumRetryIntervalMillis(),
                 new DefaultJetty93ClientCreator(6000, 6000))) {
            String content = fetch(retryHelper);
            ArrayNode records = extractArrayField(content);

            for (JsonNode record : records) {
                if (!record.isObject()) {
                    logger.warn(String.format(Locale.ENGLISH, "A record must be Json object: %s", record.toString()));
                    continue;
                }
                try {
                    recordImporter.importRecord(new JacksonServiceRecord((ObjectNode) record), pageBuilder);
                }
                catch (Exception e) {
                    logger.warn(String.format(Locale.ENGLISH, "Skipped json: %s", record.toString()), e);
                }
            }
        }

        return Exec.newTaskReport();
    }

    private ArrayNode extractArrayField(String content)
    {
        ObjectNode jsonObject = jsonParser.parseJsonObject(content);
        JsonNode jn = jsonObject.get("ids");
        if (jn.isArray()) {
            return (ArrayNode) jn;
        }
        else {
            throw new DataException("Expected array node: " + jsonObject.toString());
        }
    }

    private String fetch(Jetty93RetryHelper retryHelper)
    {
        return retryHelper.requestWithRetry(
            new StringJetty93ResponseEntityReader(5000),
            new Jetty93SingleRequester() {
                @Override
                public void requestOnce(org.eclipse.jetty.client.HttpClient client,
                                        org.eclipse.jetty.client.api.Response.Listener responseListener)
                {
                    client
                        .newRequest("http://localhost:8080/index.json")
                        .method(HttpMethod.GET)
                        .send(responseListener);
                }

                @Override
                public boolean isResponseStatusToRetry(org.eclipse.jetty.client.api.Response response)
                {
                    return response.getStatus() / 100 != 4;
                }
            });
    }

    private final Logger logger = Exec.getLogger(ExampleJetty93InputPluginDelegate.class);
}
