package org.embulk.base.restclient.record;

public abstract class ServiceValue
{
    public abstract boolean isNull();
    public abstract boolean booleanValue();
    public abstract double doubleValue();
    public abstract org.msgpack.value.Value jsonValue(org.embulk.spi.json.JsonParser jsonParser);
    public abstract long longValue();
    public abstract String stringValue();
    public abstract org.embulk.spi.time.Timestamp timestampValue(org.embulk.spi.time.TimestampParser timestampParser);
}
