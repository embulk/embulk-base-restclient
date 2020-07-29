package org.embulk.base.restclient.jackson;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.common.collect.ImmutableList;

import org.embulk.config.TaskReport;

import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ServiceRecord;

/**
 * JacksonTaskReportRecordBuffer is an implementation of {@code RecordBuffer} which includes JSON output directly in {@code TaskReport}.
 */
public class JacksonTaskReportRecordBuffer
        extends RecordBuffer
{
    public JacksonTaskReportRecordBuffer(String attributeName)
    {
        this.records = new ArrayDeque<ObjectNode>();
        this.attributeName = attributeName;
    }

    @Override
    public void bufferRecord(ServiceRecord serviceRecord)
    {
        final JacksonServiceRecord jacksonServiceRecord;
        try {
            jacksonServiceRecord = (JacksonServiceRecord) serviceRecord;
        }
        catch (ClassCastException ex) {
            throw new RuntimeException(ex);
        }
        this.records.addLast(jacksonServiceRecord.getInternalJsonNode());
    }

    @Override
    public void finish()
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public TaskReport commitWithTaskReportUpdated(TaskReport taskReport)
    {
        taskReport.set(this.attributeName, this.records);
        return taskReport;
    }

    private final ArrayDeque<ObjectNode> records;
    private final String attributeName;
}
