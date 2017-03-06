package org.embulk.base.restclient;

import java.util.List;

import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.spi.Schema;

public interface EmbulkDataEgestable<T extends RestClientOutputTaskBase>
{
    public ConfigDiff egestEmbulkData(T task, Schema schema, int taskCount, List<TaskReport> taskReports);
}
