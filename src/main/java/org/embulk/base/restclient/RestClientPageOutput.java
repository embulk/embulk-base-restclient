package org.embulk.base.restclient;

import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.RecordExporter;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.SinglePageRecordReader;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

/**
 * RestClientPageOutput is a default |PageOutput| used by |RestClientOutputPluginBase|.
 */
public class RestClientPageOutput<T extends RestClientOutputTaskBase> implements TransactionalPageOutput {
    public RestClientPageOutput(
            final Class<T> taskClass,
            final T task,
            final RecordExporter recordExporter,
            final RecordBuffer recordBuffer,
            final Schema embulkSchema,
            final int taskIndex) {
        this.taskClass = taskClass;
        this.task = task;
        this.recordExporter = recordExporter;
        this.recordBuffer = recordBuffer;
        this.embulkSchema = embulkSchema;
        this.taskIndex = taskIndex;
    }

    @Override
    public void add(final Page page) {
        final PageReader pageReader = new PageReader(this.embulkSchema);
        pageReader.setPage(page);
        while (pageReader.nextRecord()) {
            final SinglePageRecordReader singlePageRecordReader = new SinglePageRecordReader(pageReader);
            final ServiceRecord record = recordExporter.exportRecord(singlePageRecordReader);
            this.recordBuffer.bufferRecord(record);
        }
    }

    @Override
    public void finish() {
        this.recordBuffer.finish();
    }

    @Override
    public void close() {
        this.recordBuffer.close();
    }

    @Override
    public void abort() {
        // TODO(dmikurube): Implement.
    }

    @Override
    public TaskReport commit() {
        return this.recordBuffer.commitWithTaskReportUpdated(Exec.newTaskReport());
    }

    private final Class<T> taskClass;
    private final T task;
    private final RecordExporter recordExporter;
    private final RecordBuffer recordBuffer;
    private final Schema embulkSchema;
    private final int taskIndex;
}
