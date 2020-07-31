/*
 * Copyright 2017 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
public abstract class RecordBuffer {
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
