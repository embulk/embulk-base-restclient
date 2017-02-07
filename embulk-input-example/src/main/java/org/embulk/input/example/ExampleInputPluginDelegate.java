package org.embulk.input.example;

import java.util.List;
import java.util.Locale;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import org.embulk.base.restclient.request.RetryHelper;
import org.embulk.base.restclient.request.SingleRequester;
import org.embulk.base.restclient.request.StringResponseEntityReader;

import org.slf4j.Logger;

public class ExampleInputPluginDelegate
        implements RestClientInputPluginDelegate<ExampleInputPluginDelegate.PluginTask>
{
    public interface PluginTask
            extends RestClientInputTaskBase
    {
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
        return Exec.newConfigDiff();
    }

    @Override  // Overridden from |ServiceDataIngestable|
    public TaskReport ingestServiceData(final PluginTask task,
                                        RetryHelper retryHelper,
                                        RecordImporter recordImporter,
                                        int taskIndex,
                                        PageBuilder pageBuilder)
    {
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

    private String fetch(RetryHelper retryHelper)
    {
        return retryHelper.requestWithRetry(
            new StringResponseEntityReader(),
            new SingleRequester() {
                @Override
                public Response requestOnce(javax.ws.rs.client.Client client)
                {
                    final String url = String.format(Locale.ENGLISH, "http://localhost:8080");
                    return client.target(url).request().get();
                }

                @Override
                public boolean isResponseStatusToRetry(javax.ws.rs.core.Response response)
                {
                    return response.getStatus() / 100 != 4;
                }
            });
    }

    @Override  // Overridden from |ClientCreatable|
    public javax.ws.rs.client.Client createClient(PluginTask task)
    {
        // TODO(dmikurube): Configure org.glassfish.jersey.client.ClientProperties.CONNECT_TIMEOUT and READ_TIMEOUT.
        return javax.ws.rs.client.ClientBuilder.newBuilder().build();
    }

    private final Logger logger = Exec.getLogger(ExampleInputPluginDelegate.class);
}
