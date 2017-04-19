package org.embulk.base.restclient;

import org.embulk.config.TaskReport;
import org.embulk.spi.PageBuilder;

import org.embulk.base.restclient.record.RecordImporter;

public interface ServiceDataIngestable<T extends RestClientInputTaskBase>
{
    public TaskReport ingestServiceData(T task, RecordImporter recordImporter, int taskIndex, PageBuilder pageBuilder);
}
