package org.embulk.base.restclient.record;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class JacksonValueLocator
        extends ValueLocator
{
    public abstract JsonNode locateValue(ObjectNode record);
}
