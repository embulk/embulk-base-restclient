package org.embulk.base.restclient.writer;

import com.fasterxml.jackson.databind.JsonNode;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParser;

import static org.embulk.base.restclient.JacksonServiceResponseSchema.WebApiColumnOption;

public class TimestampColumnWriter
        extends AbstractColumnWriter
{
    private final TimestampParser timestampParser;

    public TimestampColumnWriter(Column column, WebApiColumnOption option, TimestampParser timestampParser)
    {
        super(column, option);
        this.timestampParser = timestampParser;
    }

    @Override
    public void write(JsonNode v, PageBuilder to)
    {
        if (v == null || v.isNull()) {
            to.setNull(column);
        }
        else {
            to.setTimestamp(column, timestampParser.parse(v.asText()));
        }
    }
}
