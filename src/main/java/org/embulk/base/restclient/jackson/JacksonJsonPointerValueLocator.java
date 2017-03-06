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
    public JsonNode seekValue(ObjectNode record)
    {
        return record.at(this.pointer);
    }

    @Override
    public void placeValue(ObjectNode record, JsonNode value)
    {
        throw new UnsupportedOperationException("placeValue is not impleented in JacksonJsonPointerValueLocator.");
    }

    private final JsonPointer pointer;
}
