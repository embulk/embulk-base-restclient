package org.embulk.base.restclient;

import org.embulk.base.restclient.record.RecordBuffer;

public interface RecordBufferBuildable<T extends RestClientOutputTaskBase>
{
    public RecordBuffer buildRecordBuffer(T task);
}
