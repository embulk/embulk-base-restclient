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
 */
public abstract class RecordBuffer
{
    public abstract void bufferRecord(ServiceRecord record);
    public abstract TaskReport commitWithTaskReportUpdated(TaskReport taskReport);
}
