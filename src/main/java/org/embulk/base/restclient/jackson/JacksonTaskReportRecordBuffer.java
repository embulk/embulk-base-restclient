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
    public TaskReport commitWithTaskReportUpdated(TaskReport taskReport)
    {
        taskReport.set(this.attributeName, this.records);
        return taskReport;
    }

    public static List<JsonNode> resumeFromTaskReport(TaskReport taskReport, String attributeName)
    {
        Iterable<Map.Entry<String, JsonNode>> attributes = taskReport.getAttributes();

        JsonNode foundAttribute = null;
        for (Map.Entry<String, JsonNode> attribute : attributes) {
            if (attribute.getKey().equals(attributeName)) {
                foundAttribute = attribute.getValue();
                break;
            }
        }
        if (foundAttribute == null) {
            throw new RuntimeException("FATAL: Unexpected format in TaskReport: " + attributeName + " not found.");
        }
        if (!foundAttribute.isArray()) {
            throw new RuntimeException("FATAL: Unexpected format in TaskReport: " + attributeName + " is not an array.");
        }

        ImmutableList.Builder<JsonNode> jsonNodesBuilder = ImmutableList.builder();
        for (JsonNode record : foundAttribute) {
            jsonNodesBuilder.add(record);
        }
        return jsonNodesBuilder.build();
    }

    private final ArrayDeque<ObjectNode> records;
    private final String attributeName;
}
