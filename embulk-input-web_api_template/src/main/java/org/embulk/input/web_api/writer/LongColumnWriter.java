package org.embulk.input.web_api.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

/**
 * Created by muga on 9/16/16.
 */
public class LongColumnWriter
        extends AbstractColumnWriter
{
    public LongColumnWriter(Column column)
    {
        super(column);
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