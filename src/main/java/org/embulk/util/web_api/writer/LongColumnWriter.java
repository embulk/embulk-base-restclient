package org.embulk.util.web_api.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import static org.embulk.util.web_api.writer.SchemaWriterFactory.WebApiColumnOption;

public class LongColumnWriter
        extends AbstractColumnWriter
{
    public LongColumnWriter(Column column, WebApiColumnOption option)
    {
        super(column, option);
    }

    @Override
    public void write(JsonNode v, PageBuilder to)
    {
        if (v == null || v.isNull()) {
            to.setNull(column);
        }
        else {
            to.setLong(column, v.longValue());
        }
    }
}
