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
