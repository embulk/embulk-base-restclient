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

package org.embulk.base.restclient;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.base.Throwables;
import java.io.IOException;
import org.embulk.base.restclient.OutputTestPluginDelegate.PluginTask;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;

public class OutputTestRecordBuffer extends RecordBuffer {
    OutputTestRecordBuffer(final String attributeName, final OutputTestPluginDelegate.PluginTask task) {
        this.attributeName = attributeName;
        this.task = task;
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
        this.records = JsonNodeFactory.instance.arrayNode();
    }

    @Override
    public void bufferRecord(final ServiceRecord serviceRecord) {
        final JacksonServiceRecord jacksonServiceRecord;
        try {
            jacksonServiceRecord = (JacksonServiceRecord) serviceRecord;
            final JsonNode record = mapper.readTree(jacksonServiceRecord.toString()).get("record");

            this.totalCount++;

            this.records.add(record);
        } catch (final ClassCastException ex) {
            throw new RuntimeException(ex);
        } catch (final IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public void finish() {
    }

    @Override
    public void close() {
    }

    @Override
    public TaskReport commitWithTaskReportUpdated(final TaskReport taskReport) {
        return Exec.newTaskReport().set("inserted", this.totalCount);
    }

    @Override
    public String toString() {
        return this.records.toString();
    }

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private final String attributeName;

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private final PluginTask task;

    private final ObjectMapper mapper;

    private ArrayNode records;
    private long totalCount;
}
