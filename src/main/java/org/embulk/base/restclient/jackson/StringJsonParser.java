package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import java.io.IOException;
import org.embulk.spi.DataException;

public class StringJsonParser {
    public StringJsonParser() {
        this.mapper = new ObjectMapper();
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
    }

    public ObjectNode parseJsonObject(final String jsonText) {
        final JsonNode node = this.parseJsonNode(jsonText);
        if (node.isObject()) {
            return (ObjectNode) node;
        } else {
            throw new DataException("Expected object node: " + jsonText);
        }
    }

    public ArrayNode parseJsonArray(final String jsonText) {
        final JsonNode node = this.parseJsonNode(jsonText);
        if (node.isArray()) {
            return (ArrayNode) node;
        } else {
            throw new DataException("Expected array node: " + jsonText);
        }
    }

    private JsonNode parseJsonNode(final String jsonText) {
        try {
            return mapper.readTree(jsonText);
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private final ObjectMapper mapper;
}
