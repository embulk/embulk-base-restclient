package org.embulk.base.restclient;

import org.embulk.spi.PageBuilder;

import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.request.RetryHelper;

public interface ServiceDataIngestable<T extends RestClientInputTaskBase>
{
    public void ingestServiceData(T task, RetryHelper retryHelper, RecordImporter recordImporter, int taskCount, PageBuilder pageBuilder);
}
