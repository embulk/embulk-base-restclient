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

package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayDeque;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.TaskReport;

/**
 * JacksonTaskReportRecordBuffer is an implementation of {@code RecordBuffer} which includes JSON output directly in {@code TaskReport}.
 */
public class JacksonTaskReportRecordBuffer extends RecordBuffer {
    public JacksonTaskReportRecordBuffer(final String attributeName) {
        this.records = new ArrayDeque<ObjectNode>();
        this.attributeName = attributeName;
    }

    @Override
    public void bufferRecord(final ServiceRecord serviceRecord) {
        final JacksonServiceRecord jacksonServiceRecord;
        try {
            jacksonServiceRecord = (JacksonServiceRecord) serviceRecord;
        } catch (final ClassCastException ex) {
            throw new RuntimeException(ex);
        }
        this.records.addLast(jacksonServiceRecord.getInternalJsonNode());
    }

    @Override
    public void finish() {
    }

    @Override
    public void close() {
    }

    @Override
    public TaskReport commitWithTaskReportUpdated(final TaskReport taskReport) {
        taskReport.set(this.attributeName, this.records);
        return taskReport;
    }

    private final ArrayDeque<ObjectNode> records;
    private final String attributeName;
}
