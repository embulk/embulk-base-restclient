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

import java.time.Instant;
import org.embulk.spi.Schema;

public abstract class TimestampServiceDataSplitter<T extends RestClientInputTaskBase> extends ServiceDataSplitter<T> {
    @Override
    public final int numberToSplitWithHintingInTask(final T taskToHint) {
        final Instant beginningOfOverall = this.getBeginningOfOverall(taskToHint);
        final Instant endingOfOverall = this.getEndingOfOverall(taskToHint);
        final SplitCalculator calculator = this.getSplitCalculator(taskToHint);
        return calculator.calculateNumberToSplit(beginningOfOverall, endingOfOverall);
    }

    @Override
    public final void hintInEachSplitTask(final T taskToHint, final Schema schema, final int taskIndex) {
        final Instant beginningOfOverall = this.getBeginningOfOverall(taskToHint);
        final Instant endingOfOverall = this.getEndingOfOverall(taskToHint);
        final SplitCalculator calculator = this.getSplitCalculator(taskToHint);

        this.setTimestampsOfEachTask(
                taskToHint,
                calculator.calculateBeginningOfEachTask(beginningOfOverall, endingOfOverall, taskIndex),
                calculator.calculateEndingOfEachTask(beginningOfOverall, endingOfOverall, taskIndex));
    }

    public abstract static class SplitCalculator {
        public abstract int calculateNumberToSplit(Instant beginning, Instant ending);

        public abstract Instant calculateBeginningOfEachTask(
                Instant beginningOfOverall,
                Instant endingOfOverall,
                int taskIndex);

        public abstract Instant calculateEndingOfEachTask(
                Instant beginningOfOverall,
                Instant endingOfOverall,
                int taskIndex);
    }

    // TODO: Implement some example SplitCalculator such as WEEKLY, MONTHLY, YEARLY, ...

    /**
     * Returns the overall beginning time from the given Task.
     *
     * This method is to be implemented in plugin's concrete ServiceDataSplitter class.
     * Called only from |numberToSplitWithHintingInTask| and |hintInEachSplitTask|.
     */
    protected abstract Instant getBeginningOfOverall(T task);

    /**
     * Returns the overall endning time from the given Task.
     *
     * This method is to be implemented in plugin's concrete ServiceDataSplitter class.
     * Called only from |numberToSplitWithHintingInTask| and |hintInEachSplitTask|.
     */
    protected abstract Instant getEndingOfOverall(T task);

    /**
     * Returns an appropriate SplitCalculator for the given Task.
     *
     * This method is to be implemented in plugin's concrete ServiceDataSplitter class.
     * Called only from |numberToSplitWithHintingInTask| and |hintInEachSplitTask|.
     */
    protected abstract SplitCalculator getSplitCalculator(T task);

    /**
     * Sets per-task beginning and ending times in the Task.
     *
     * This method is to be implemented in plugin's concrete ServiceDataSplitter class.
     * Called only from |numberToSplitWithHintingInTask| and |hintInEachSplitTask|.
     */
    protected abstract void setTimestampsOfEachTask(T task, Instant beginning, Instant ending);
}
