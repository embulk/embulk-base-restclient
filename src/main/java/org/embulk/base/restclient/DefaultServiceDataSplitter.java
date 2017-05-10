package org.embulk.base.restclient;

import org.embulk.config.TaskSource;
import org.embulk.spi.Schema;

public class DefaultServiceDataSplitter<T extends RestClientInputTaskBase>
        extends ServiceDataSplitter<T>
{
    @Override
    public int numberToSplitWithHintingInTask(T taskToHint)
    {
        return 1;
    }

    @Override
    public void hintInEachSplitTask(T taskToHint, Schema schema, int taskIndex)
    {
    }
}
