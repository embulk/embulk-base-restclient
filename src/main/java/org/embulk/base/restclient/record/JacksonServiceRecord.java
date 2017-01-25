package org.embulk.base.restclient.record;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class JacksonServiceRecord
        extends ServiceRecord
{
    public JacksonServiceRecord(ObjectNode record)
    {
        this.record = record;
    }

    @Override
    public JacksonServiceValue getValue(ValueLocator locator)
    {
        if (locator instanceof JacksonValueLocator) {
            JacksonValueLocator jacksonLocator = (JacksonValueLocator) locator;
            return new JacksonServiceValue(jacksonLocator.locateValue(record));
        }
        throw new RuntimeException("Non-JacksonValueLocator is used to locate a value in JacksonServiceRecord");
    }

    private ObjectNode record;
}
