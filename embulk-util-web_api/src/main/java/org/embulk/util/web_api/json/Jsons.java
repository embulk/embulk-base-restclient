package org.embulk.util.web_api.json;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;

public class Jsons
{
    public static JsonNode parseJsonObject(String jsonText)
            throws IOException
    {
        return newObjectMapper().readTree(jsonText);
    }

    public static ArrayNode parseJsonArray(String jsonText)
            throws IOException
    {
        return (ArrayNode) parseJsonObject(jsonText);
    }

    public static ObjectMapper newObjectMapper()
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
        return mapper;
    }
}
