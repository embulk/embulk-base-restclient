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
