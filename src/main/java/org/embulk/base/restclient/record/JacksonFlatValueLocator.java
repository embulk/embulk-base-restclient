package org.embulk.base.restclient.record;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JacksonFlatValueLocator
        extends JacksonValueLocator
{
    public JacksonFlatValueLocator(String name)
    {
        this.name = name;
    }

    @Override
    public JsonNode locateValue(ObjectNode record)
    {
        return record.get(this.name);
    }

    private String name;
}
