package org.embulk.base.restclient;

import org.embulk.spi.Schema;

public class DefaultServiceDataSplitter<T extends RestClientInputTaskBase> extends ServiceDataSplitter<T> {
    @Override
    public int numberToSplitWithHintingInTask(final T taskToHint) {
        return 1;
    }

    @Override
    public void hintInEachSplitTask(final T taskToHint, final Schema schema, final int taskIndex) {
    }
}
