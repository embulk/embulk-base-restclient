package org.embulk.util.web_api.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParser;
import org.embulk.util.web_api.writer.SchemaWriterFactory.WebApiColumnOption;

public class JsonColumnWriter
        extends AbstractColumnWriter
{
    private final JsonParser jsonParser;

    public JsonColumnWriter(Column column, WebApiColumnOption option, JsonParser jsonParser)
    {
        super(column, option);
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
