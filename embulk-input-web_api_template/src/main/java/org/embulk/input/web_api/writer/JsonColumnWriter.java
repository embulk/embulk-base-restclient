package org.embulk.input.web_api.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParser;

public class JsonColumnWriter
        extends AbstractColumnWriter
{
    private final JsonParser jsonParser;

    public JsonColumnWriter(Column column, JsonParser jsonParser)
    {
        super(column);
        this.jsonParser = jsonParser;
    }

    @Override
    public void write(JsonNode v, PageBuilder to)
    {
        if (v == null || v.isNull()) {
            to.setNull(column);
        }
        else {
            to.setJson(column, jsonParser.parse(v.toString()));
        }
    }
}
