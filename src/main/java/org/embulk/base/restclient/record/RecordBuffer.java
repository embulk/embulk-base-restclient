package org.embulk.base.restclient.record;

import org.embulk.config.TaskReport;

public abstract class RecordBuffer
{
    public abstract void bufferRecord(ServiceRecord record);
    public abstract TaskReport commitWithTaskReportUpdated(TaskReport taskReport);
}
