package org.embulk.base.restclient;

import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.config.TaskReport;
import org.embulk.spi.PageBuilder;

public interface ServiceDataIngestable<T extends RestClientInputTaskBase> {
    TaskReport ingestServiceData(T task, RecordImporter recordImporter, int taskIndex, PageBuilder pageBuilder);
}
