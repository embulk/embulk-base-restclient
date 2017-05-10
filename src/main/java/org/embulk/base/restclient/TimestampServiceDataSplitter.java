package org.embulk.base.restclient;

import org.embulk.config.TaskSource;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;

public abstract class TimestampServiceDataSplitter<T extends RestClientInputTaskBase>
        extends ServiceDataSplitter<T>
{
    @Override
    public final int numberToSplitWithHintingInTask(T taskToHint)
    {
        final Timestamp beginningOfOverall = this.getBeginningOfOverall(taskToHint);
        final Timestamp endingOfOverall = this.getEndingOfOverall(taskToHint);
        final SplitCalculator calculator = this.getSplitCalculator(taskToHint);
        return calculator.calculateNumberToSplit(beginningOfOverall, endingOfOverall);
    }

    @Override
    public final void hintInEachSplitTask(T taskToHint, Schema schema, int taskIndex)
    {
        final Timestamp beginningOfOverall = this.getBeginningOfOverall(taskToHint);
        final Timestamp endingOfOverall = this.getEndingOfOverall(taskToHint);
        final SplitCalculator calculator = this.getSplitCalculator(taskToHint);

        this.setTimestampsOfEachTask(
            taskToHint,
            calculator.calculateBeginningOfEachTask(beginningOfOverall, endingOfOverall, taskIndex),
            calculator.calculateEndingOfEachTask(beginningOfOverall, endingOfOverall, taskIndex));
    }

    public static abstract class SplitCalculator
    {
        public abstract int calculateNumberToSplit(final Timestamp beginning, final Timestamp ending);

        public abstract Timestamp calculateBeginningOfEachTask(
            final Timestamp beginningOfOverall,
            final Timestamp endingOfOverall,
            final int taskIndex);

        public abstract Timestamp calculateEndingOfEachTask(
            final Timestamp beginningOfOverall,
            final Timestamp endingOfOverall,
            final int taskIndex);
    }

    // TODO: Implement some example SplitCalculator such as WEEKLY, MONTHLY, YEARLY, ...

    /**
     * Returns the overall beginning time from the given Task.
     *
     * This method is to be implemented in plugin's concrete ServiceDataSplitter class.
     * Called only from |numberToSplitWithHintingInTask| and |hintInEachSplitTask|.
     */
    protected abstract Timestamp getBeginningOfOverall(final T task);

    /**
     * Returns the overall endning time from the given Task.
     *
     * This method is to be implemented in plugin's concrete ServiceDataSplitter class.
     * Called only from |numberToSplitWithHintingInTask| and |hintInEachSplitTask|.
     */
    protected abstract Timestamp getEndingOfOverall(final T task);

    /**
     * Returns an appropriate SplitCalculator for the given Task.
     *
     * This method is to be implemented in plugin's concrete ServiceDataSplitter class.
     * Called only from |numberToSplitWithHintingInTask| and |hintInEachSplitTask|.
     */
    protected abstract SplitCalculator getSplitCalculator(final T task);

    /**
     * Sets per-task beginning and ending times in the Task.
     *
     * This method is to be implemented in plugin's concrete ServiceDataSplitter class.
     * Called only from |numberToSplitWithHintingInTask| and |hintInEachSplitTask|.
     */
    protected abstract void setTimestampsOfEachTask(T task, final Timestamp beginning, final Timestamp ending);
}
