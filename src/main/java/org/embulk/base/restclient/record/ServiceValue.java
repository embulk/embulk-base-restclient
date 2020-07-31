package org.embulk.base.restclient.record;

import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.msgpack.value.Value;

public abstract class ServiceValue {
    public abstract boolean isNull();

    public abstract boolean booleanValue();

    public abstract double doubleValue();

    public abstract Value jsonValue(JsonParser jsonParser);

    public abstract long longValue();

    public abstract String stringValue();

    public abstract Timestamp timestampValue(TimestampParser timestampParser);
}
