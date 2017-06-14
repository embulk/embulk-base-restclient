package org.embulk.base.restclient.record;

import org.embulk.config.TaskReport;

/**
 * RecordBuffer buffers output from each task in an output plugin.
 *
 * Records are output in each task (slave) from Embulk possibly in parallel.
 * Slaves may run remote, and the outputs are finally committed in the entire {@code transaction} (master).
 *
 * Note that only {@code TaskReport} is a direct channel from slaves to a master.
 *
 * RecordBuffer works as a bridge from a slave to a master. The implementation depends on the destination,
 * for example:
 *
 * - Include all the output directly in {@code TaskReport}. Note that {@code TaskReport} can bloat.
 * - Put the output to an external storage (e.g. S3), and include a pointer to the storage in {@code TaskReport}.
 *
 * Or, it is possible to commit directly from {@code RecordBuffer}. But, the destination must accept parallel
 * uploads, and developers may need to take care of transactions and orders.
 *
 * If {@code RecordBuffer} owns external resources for direct uploading, the resources need to be released in
 * its {@code close} or {@code finish}. Releasing resources in {@code close} is the typical manner.
 */
public abstract class RecordBuffer
{
    public abstract void bufferRecord(ServiceRecord record);

    /**
     * Finishes the {@code RecordBuffer}.
     *
     * This method is called when {@code RestClientPageOutput#finish} is called. Implement this method usually to
     * finish resources managed in {@code RecordBuffer}.
     */
    public abstract void finish();

    /**
     * Closes the {@code RecordBuffer}.
     *
     * This method is called when {@code RestClientPageOutput#close} is called. Implement this method usually to
     * close resources managed in {@code RecordBuffer}.
     */
    public abstract void close();

    public abstract TaskReport commitWithTaskReportUpdated(TaskReport taskReport);
}
