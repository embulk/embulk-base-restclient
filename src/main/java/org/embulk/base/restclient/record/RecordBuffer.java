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
 * uploads, and developers may need to take care of transactions and orders. If {@code RecordBuffer} might own
 * external resource for uploads, it might need to release the resource. The resource can be released in the
 * finish or close method. In almost all cases, the external resource might have same
 * lifecycle as {@code RecordBuffer}'s. The close method might be used for releasing the resource. The method is
 * called when {@code PageOutput} (and {@code RecordBuffer}) is closed. If the resource needs to be released when
 * {@code PageOutput} finishes, the finish method needs to be used.
 */
public abstract class RecordBuffer
{
    public abstract void bufferRecord(ServiceRecord record);
    public abstract void finish();
    public abstract void close();
    public abstract TaskReport commitWithTaskReportUpdated(TaskReport taskReport);
}
