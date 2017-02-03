package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import org.embulk.spi.DataException;

import java.io.IOException;

public class StringJsonParser
{
    private final ObjectMapper mapper;

    public StringJsonParser()
    {
        this.mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
    }

    public ObjectNode parseJsonObject(String jsonText)
    {
        JsonNode node = parseJsonNode(jsonText);
        if (node.isObject()) {
            return (ObjectNode) node;
        }
        else {
            throw new DataException("Expected object node: " + jsonText);
        }
    }

    public ArrayNode parseJsonArray(String jsonText)
    {
        JsonNode node = parseJsonNode(jsonText);
        if (node.isArray()) {
            return (ArrayNode) node;
        }
        else {
            throw new DataException("Expected array node: " + jsonText);
        }
    }

    private JsonNode parseJsonNode(String jsonText)
    {
        try {
            return mapper.readTree(jsonText);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
