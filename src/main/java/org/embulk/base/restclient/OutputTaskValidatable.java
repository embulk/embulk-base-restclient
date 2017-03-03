package org.embulk.base.restclient;

import org.embulk.spi.Schema;

public interface OutputTaskValidatable<T extends RestClientOutputTaskBase>
{
    public void validateOutputTask(T task, Schema embulkSchema, int taskCount);
}
