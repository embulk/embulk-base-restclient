/*
 * Copyright 2016 The Embulk project
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

package org.embulk.output.example;

import java.util.Calendar;
import java.util.List;
import java.util.Locale;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;

import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.jackson.JacksonJsonPointerValueLocator;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonTaskReportRecordBuffer;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.jackson.scope.JacksonAllInObjectScope;
import org.embulk.base.restclient.jackson.scope.JacksonDirectIntegerScope;
import org.embulk.base.restclient.jackson.scope.JacksonDirectStringScope;
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
        @Config("endpoint")
        public String getEndpoint();

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

    @Override  // Overridden from |OutputTaskValidatable|
    public void validateOutputTask(PluginTask task, Schema embulkSchema, int taskCount)
    {
    }

    @Override  // Overridden from |ServiceRequestMapperBuildable|
    public JacksonServiceRequestMapper buildServiceRequestMapper(PluginTask task)
    {
        return JacksonServiceRequestMapper.builder()
            .add(new JacksonDirectIntegerScope("id"), new JacksonTopLevelValueLocator("id"))
            .add(new JacksonDirectStringScope("name"), new JacksonTopLevelValueLocator("name"))
            .addNewObject(new JacksonTopLevelValueLocator("dict"))
            .addNewObject(new JacksonJsonPointerValueLocator("/dict/sub1"))
            .addNewObject(new JacksonJsonPointerValueLocator("/dict/sub1/sub2"))
            .add(new JacksonDirectStringScope("foo"), new JacksonJsonPointerValueLocator("/dict/sub1/foo"))
            .add(new JacksonDirectStringScope("bar"), new JacksonJsonPointerValueLocator("/dict/sub1/sub2/bar"))
            .add(new JacksonAllInObjectScope(), new JacksonTopLevelValueLocator("entire"))
            .build();
    }

    @Override  // Overridden from |RecordBufferBuildable|
    public RecordBuffer buildRecordBuffer(PluginTask task, Schema schema, int taskIndex)
    {
        return new JacksonTaskReportRecordBuffer("records");
    }

    @Override  // Overridden from |EmbulkDataEgestable|
    public ConfigDiff egestEmbulkData(final PluginTask task,
                                      Schema schema,
                                      int taskIndex,
                                      List<TaskReport> taskReports)
    {
        ArrayNode records = OBJECT_MAPPER.createArrayNode();
        for (TaskReport taskReport : taskReports) {
            final List<?> reportRecords = taskReport.get(List.class, "records");
            for (final Object reportRecord : reportRecords) {
                if (reportRecord instanceof JsonNode) {
                    records.add((JsonNode) reportRecord);
                } else {
                    throw new RuntimeException("TaskReport: \"record\" does not consist of JSON nodes.");
                }
            }
        }

        ObjectNode json = OBJECT_MAPPER.createObjectNode();
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
            push(retryHelper, json, task.getEndpoint());
        }

        return Exec.newConfigDiff();
    }

    private String push(JAXRSRetryHelper retryHelper, final JsonNode json, final String endpoint)
    {
        return retryHelper.requestWithRetry(
            new StringJAXRSResponseEntityReader(),
            new JAXRSSingleRequester() {
                @Override
                public Response requestOnce(javax.ws.rs.client.Client client)
                {
                    return client.target(endpoint).request().post(
                        Entity.<String>entity(json.toString(), MediaType.APPLICATION_JSON));
                }

                @Override
                public boolean isResponseStatusToRetry(javax.ws.rs.core.Response response)
                {
                    return response.getStatus() / 100 != 4;
                }
            });
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
}
