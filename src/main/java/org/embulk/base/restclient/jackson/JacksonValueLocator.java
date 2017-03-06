package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.embulk.base.restclient.record.ValueLocator;

public abstract class JacksonValueLocator
        extends ValueLocator
{
    public abstract JsonNode seekValue(ObjectNode record);
    public abstract void placeValue(ObjectNode record, JsonNode value);
}
