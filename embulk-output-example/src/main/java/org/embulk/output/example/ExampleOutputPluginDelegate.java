package org.embulk.output.example;

import java.util.Calendar;
import java.util.List;
import java.util.Locale;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;

import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonStraightStringValueScope;
import org.embulk.base.restclient.jackson.JacksonTaskReportRecordBuffer;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.record.RecordBuffer;

import org.embulk.util.retryhelper.jaxrs.JAXRSClientCreator;
import org.embulk.util.retryhelper.jaxrs.JAXRSRetryHelper;
import org.embulk.util.retryhelper.jaxrs.JAXRSSingleRequester;
import org.embulk.util.retryhelper.jaxrs.StringJAXRSResponseEntityReader;

public class ExampleOutputPluginDelegate
    implements RestClientOutputPluginDelegate<ExampleOutputPluginDelegate.PluginTask>
{
    public interface PluginTask
            extends RestClientOutputTaskBase
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

    @Override  // Overridden from |TaskValidatable|
    public void validateTask(PluginTask task)
    {
    }

    // @Override  // Overridden from |ServiceRequestMapperBuildable|
    public JacksonServiceRequestMapper buildServiceRequestMapper(PluginTask task)
    {
        return JacksonServiceRequestMapper.builder()
            .add(new JacksonStraightStringValueScope("name"), new JacksonTopLevelValueLocator("name"))
            .build();
    }

    @Override  // Overridden from |RecordBufferBuildable|
    public RecordBuffer buildRecordBuffer(PluginTask task)
    {
        return new JacksonTaskReportRecordBuffer("records");
    }

    @Override  // Overridden from |EmbulkDataEgestable|
    public ConfigDiff egestEmbulkData(final PluginTask task,
                                      Schema schema,
                                      int taskIndex,
                                      List<TaskReport> taskReports)
    {
        ArrayNode records = JsonNodeFactory.instance.arrayNode();
        for (TaskReport taskReport : taskReports) {
            records.addAll(JacksonTaskReportRecordBuffer.resumeFromTaskReport(taskReport, "records"));
        }

        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put("timestamp", Calendar.getInstance().getTime().getTime());
        json.set("records", records);

        try (JAXRSRetryHelper retryHelper = new JAXRSRetryHelper(
                 task.getMaximumRetries(),
                 task.getInitialRetryIntervalMillis(),
                 task.getMaximumRetryIntervalMillis(),
                 new JAXRSClientCreator() {
                     @Override
                     public javax.ws.rs.client.Client create() {
                         return javax.ws.rs.client.ClientBuilder.newBuilder().build();
                     }
                 })) {
            push(retryHelper, json);
        }

        return Exec.newConfigDiff();
    }

    private String push(JAXRSRetryHelper retryHelper, final JsonNode json)
    {
        return retryHelper.requestWithRetry(
            new StringJAXRSResponseEntityReader(),
            new JAXRSSingleRequester() {
                @Override
                public Response requestOnce(javax.ws.rs.client.Client client)
                {
                    final String url = String.format(Locale.ENGLISH, "http://localhost:8080");
                    return client.target(url).request().post(
                        Entity.<String>entity(json.toString(), MediaType.APPLICATION_JSON));
                }

                @Override
                public boolean isResponseStatusToRetry(javax.ws.rs.core.Response response)
                {
                    return response.getStatus() / 100 != 4;
                }
            });
    }
}
