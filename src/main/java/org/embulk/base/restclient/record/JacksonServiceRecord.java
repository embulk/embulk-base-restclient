package org.embulk.base.restclient.record;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class JacksonServiceRecord
        extends ServiceRecord<JacksonValueLocator>
{
    public JacksonServiceRecord(ObjectNode record)
    {
        this.record = record;
    }

    @Override
    public JacksonServiceValue getValue(JacksonValueLocator locator)
    {
        return new JacksonServiceValue(locator.locateValue(record));
    }

    private ObjectNode record;
}
