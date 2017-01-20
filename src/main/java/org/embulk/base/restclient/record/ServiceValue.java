package org.embulk.base.restclient.record;

abstract public class ServiceValue
{
    abstract public boolean isNull();
    abstract public boolean booleanValue();
    abstract public double doubleValue();
    abstract public org.msgpack.value.Value jsonValue(org.embulk.spi.json.JsonParser jsonParser);
    abstract public long longValue();
    abstract public String stringValue();
    abstract public org.embulk.spi.time.Timestamp timestampValue(org.embulk.spi.time.TimestampParser timestampParser);
}
