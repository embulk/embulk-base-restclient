package org.embulk.base.restclient.record;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

abstract public class JacksonValueLocator
        extends ValueLocator
{
    abstract public JsonNode locateValue(ObjectNode record);
}
