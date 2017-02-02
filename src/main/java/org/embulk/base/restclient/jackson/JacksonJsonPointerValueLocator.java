package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JacksonJsonPointerValueLocator
        extends JacksonValueLocator
{
    public JacksonJsonPointerValueLocator(String pointerString)
    {
        this.pointer = JsonPointer.compile(pointerString);
    }

    public JacksonJsonPointerValueLocator(JsonPointer pointer)
    {
        this.pointer = pointer;
    }

    @Override
    public JsonNode locateValue(ObjectNode record)
    {
        return record.at(this.pointer);
    }

    private final JsonPointer pointer;
}
