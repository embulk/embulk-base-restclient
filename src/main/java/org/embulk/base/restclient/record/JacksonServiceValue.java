package org.embulk.base.restclient.record;

import com.fasterxml.jackson.databind.JsonNode;

public class JacksonServiceValue
        extends ServiceValue
{
    public JacksonServiceValue(JsonNode value)
    {
        this.value = value;
    }

    @Override
    public boolean isNull()
    {
        return value.isNull();
    }

    @Override
    public boolean booleanValue()
    {
        return value.booleanValue();
    }

    @Override
    public double doubleValue()
    {
        return value.doubleValue();
    }

    @Override
    public org.msgpack.value.Value jsonValue(org.embulk.spi.json.JsonParser jsonParser)
    {
        return jsonParser.parse(value.toString());
    }

    @Override
    public long longValue()
    {
        return value.longValue();
    }

    @Override
    public String stringValue()
    {
        return value.asText();
    }

    @Override
    public org.embulk.spi.time.Timestamp timestampValue(org.embulk.spi.time.TimestampParser timestampParser)
    {
        return timestampParser.parse(value.asText());
    }

    private JsonNode value;
}
