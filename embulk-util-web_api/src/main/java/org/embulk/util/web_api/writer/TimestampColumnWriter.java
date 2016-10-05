package org.embulk.util.web_api.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParser;

public class TimestampColumnWriter
        extends AbstractColumnWriter
{
    private final TimestampParser timestampParser;

    public TimestampColumnWriter(Column column, ConfigSource config, TimestampParser timestampParser)
    {
        super(column, config);
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
