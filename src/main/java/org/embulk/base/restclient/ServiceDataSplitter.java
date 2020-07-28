/*
 * Copyright 2016 The Embulk project
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
