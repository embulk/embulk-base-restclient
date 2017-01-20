package org.embulk.base.restclient.writer;

import com.fasterxml.jackson.databind.JsonNode;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import org.embulk.base.restclient.writer.SchemaWriterFactory.WebApiColumnOption;

public class DoubleColumnWriter
        extends AbstractColumnWriter
{
    public DoubleColumnWriter(Column column, WebApiColumnOption option)
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
            to.setDouble(column, v.doubleValue());
        }
    }
}
