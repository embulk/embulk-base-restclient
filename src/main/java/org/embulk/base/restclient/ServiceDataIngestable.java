package org.embulk.base.restclient;

import org.embulk.spi.PageBuilder;

import org.embulk.base.restclient.request.RetryHelper;
import org.embulk.base.restclient.writer.SchemaWriter;

public interface ServiceDataIngestable<T extends RestClientInputTaskBase>
{
    public void ingestServiceData(T task, RetryHelper retryHelper, SchemaWriter schemaWriter, int taskCount, PageBuilder pageBuilder);
}
