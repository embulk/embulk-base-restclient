package org.embulk.base.restclient;

import org.embulk.spi.Schema;

/**
 * ServiceDataSplitter controls how the data are split into multiple Embulk tasks.
 *
 * ServiceDataSplitter just calculates how many tasks to split, and provides hints to
 * {@code ServideDataIngestable#ingestServiceData} through the plugin's {@code Task}.
 */
public abstract class ServiceDataSplitter<T extends RestClientInputTaskBase> {
    /**
     * Calculates and returns how many tasks to split for the given Config.
     *
     * It may add some hint entries in the given Task.
     *
     * It runs in {@code InputPlugin#transaction} just once before splitting into parallel tasks.
     *
     * NOTE: This |numberToSplitWithHintingInTask| cannot share fields with |hintInEachSplitTask|.
     */
    public abstract int numberToSplitWithHintingInTask(T taskToHint);

    /**
     * Adds hint entries in the given split Task.
     *
     * It runs in {@code InputPlugin#run} in each task after splitting.
     *
     * NOTE: This |numberToSplitWithHintingInTask| cannot share fields with |hintInEachSplitTask|.
     * |numberToSplitWithHintingInTask| runs in |transaction|, and |hintInEachSplitTask| runs in |run|.
     */
    public abstract void hintInEachSplitTask(T taskToHint, Schema schema, int taskIndex);
}
