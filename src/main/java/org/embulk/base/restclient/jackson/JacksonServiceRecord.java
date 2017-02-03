package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;

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
        // Based on the implementation of |JacksonServiceResponseSchema|,
        // the |ValueLocator| should be always |JacksonValueLocator|.
        // The class cast should always success.
        //
        // NOTE: Just casting (with catching |ClassCastException|) is
        // faster than checking (instanceof) and then casting if
        // the |ClassCastException| never or hardly happens.
        JacksonValueLocator jacksonLocator;
        try {
            jacksonLocator = (JacksonValueLocator) locator;
        } catch (ClassCastException ex) {
            throw new RuntimeException("Non-JacksonValueLocator is used to locate a value in JacksonServiceRecord", ex);
        }
        return new JacksonServiceValue(jacksonLocator.locateValue(record));
    }

    private ObjectNode record;
}
