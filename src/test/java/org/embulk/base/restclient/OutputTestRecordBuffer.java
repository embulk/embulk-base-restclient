package org.embulk.base.restclient;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.base.Throwables;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;

import org.embulk.base.restclient.OutputTestPluginDelegate.PluginTask;

import java.io.IOException;

public class OutputTestRecordBuffer
        extends RecordBuffer {

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private final String attributeName;
    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private final PluginTask task;
    private final ObjectMapper mapper;
    private ArrayNode records;
    private long totalCount;

    OutputTestRecordBuffer(String attributeName, OutputTestPluginDelegate.PluginTask task)
    {
        this.attributeName = attributeName;
        this.task = task;
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
        this.records = JsonNodeFactory.instance.arrayNode();
    }

    @Override
    public void bufferRecord(ServiceRecord serviceRecord)
    {
        JacksonServiceRecord jacksonServiceRecord;
        try {
            jacksonServiceRecord = (JacksonServiceRecord) serviceRecord;
            JsonNode record = mapper.readTree(jacksonServiceRecord.toString()).get("record");

            totalCount++;

            records.add(record);
        }
        catch (ClassCastException ex) {
            throw new RuntimeException(ex);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
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
        return Exec.newTaskReport().set("inserted", totalCount);
    }

    @Override
    public String toString()
    {
        return this.records.toString();
    }
}