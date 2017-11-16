package org.embulk.base.restclient;

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

import java.util.ArrayList;
import java.util.List;

public class OutputTestPluginDelegate
        implements RestClientOutputPluginDelegate<OutputTestPluginDelegate.PluginTask>
{

    private static ArrayList<OutputTestRecordBuffer> buffers;

    OutputTestPluginDelegate(boolean outputNulls)
    {
        buffers = new ArrayList<>();
        this.outputNulls = outputNulls;
    }

    public interface PluginTask
            extends RestClientOutputTaskBase, TimestampFormatter.Task
    {

    }

    @Override  // Overridden from |OutputTaskValidatable|
    public void validateOutputTask(PluginTask task, Schema embulkSchema, int taskCount)
    {

    }

    @Override  // Overridden from |ServiceRequestMapperBuildable|
    public JacksonServiceRequestMapper buildServiceRequestMapper(PluginTask task)
    {
        TimestampFormatter formatter = new TimestampFormatter("%Y-%m-%dT%H:%M:%S.%3N%z", DateTimeZone.forID("UTC"));

        return JacksonServiceRequestMapper.builder()
                .add(new JacksonAllInObjectScope(formatter, this.outputNulls), new JacksonTopLevelValueLocator("record"))
                .build();
    }

    @Override  // Overridden from |RecordBufferBuildable|
    public RecordBuffer buildRecordBuffer(PluginTask task, Schema schema, int taskIndex)
    {
        OutputTestRecordBuffer buffer = new OutputTestRecordBuffer("records", task);
        OutputTestPluginDelegate.buffers.add(buffer);
        return buffer;
    }

    @Override
    public ConfigDiff egestEmbulkData(final PluginTask task,
                                      Schema schema,
                                      int taskIndex,
                                      List<TaskReport> taskReports)
    {
        return Exec.newConfigDiff();
    }

    static ArrayList<OutputTestRecordBuffer> getBuffers() {
        return OutputTestPluginDelegate.buffers;
    }

    private boolean outputNulls;
}
