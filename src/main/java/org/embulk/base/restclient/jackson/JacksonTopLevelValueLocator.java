package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JacksonTopLevelValueLocator extends JacksonValueLocator {
    public JacksonTopLevelValueLocator(final String name) {
        this.name = name;
    }

    @Override
    public JsonNode seekValue(final ObjectNode record) {
        return record.get(this.name);
    }

    @Override
    public void placeValue(final ObjectNode record, final JsonNode value) {
        record.set(this.name, value);
    }

    private String name;
}
