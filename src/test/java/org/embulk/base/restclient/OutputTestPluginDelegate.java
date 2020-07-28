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

import java.util.ArrayList;
import java.util.List;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.jackson.scope.JacksonAllInObjectScope;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampFormatter;
import org.joda.time.DateTimeZone;

public class OutputTestPluginDelegate implements RestClientOutputPluginDelegate<OutputTestPluginDelegate.PluginTask> {
    OutputTestPluginDelegate(final boolean outputNulls) {
        buffers = new ArrayList<>();
        this.outputNulls = outputNulls;
    }

    public interface PluginTask extends RestClientOutputTaskBase, TimestampFormatter.Task {
    }

    @Override  // Overridden from |OutputTaskValidatable|
    public void validateOutputTask(final PluginTask task, final Schema embulkSchema, final int taskCount) {
    }

    @Override  // Overridden from |ServiceRequestMapperBuildable|
    public JacksonServiceRequestMapper buildServiceRequestMapper(final PluginTask task) {
        final TimestampFormatter formatter = new TimestampFormatter("%Y-%m-%dT%H:%M:%S.%3N%z", DateTimeZone.forID("UTC"));

        return JacksonServiceRequestMapper.builder()
                .add(new JacksonAllInObjectScope(formatter, this.outputNulls), new JacksonTopLevelValueLocator("record"))
                .build();
    }

    @Override  // Overridden from |RecordBufferBuildable|
    public RecordBuffer buildRecordBuffer(final PluginTask task, final Schema schema, final int taskIndex) {
        final OutputTestRecordBuffer buffer = new OutputTestRecordBuffer("records", task);
        OutputTestPluginDelegate.buffers.add(buffer);
        return buffer;
    }

    @Override
    public ConfigDiff egestEmbulkData(
            final PluginTask task,
            final Schema schema,
            final int taskIndex,
            final List<TaskReport> taskReports) {
        return Exec.newConfigDiff();
    }

    static ArrayList<OutputTestRecordBuffer> getBuffers() {
        return OutputTestPluginDelegate.buffers;
    }

    private static ArrayList<OutputTestRecordBuffer> buffers;

    private final boolean outputNulls;
}
