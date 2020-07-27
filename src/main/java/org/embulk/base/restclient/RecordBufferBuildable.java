package org.embulk.base.restclient;

import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.spi.Schema;

public interface RecordBufferBuildable<T extends RestClientOutputTaskBase> {
    RecordBuffer buildRecordBuffer(T task, Schema schema, int taskIndex);
}
