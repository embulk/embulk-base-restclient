package org.embulk.base.restclient.writer;

import com.fasterxml.jackson.databind.JsonNode;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import static org.embulk.base.restclient.writer.SchemaWriterFactory.*;

public class BooleanColumnWriter
        extends AbstractColumnWriter
{
    public BooleanColumnWriter(Column column, WebApiColumnOption option)
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
            to.setBoolean(column, v.booleanValue());
        }
    }
}
