package org.embulk.util.web_api.writer;

import org.embulk.config.ConfigSource;
import org.embulk.spi.Column;

abstract class AbstractColumnWriter
        implements ColumnWriter
{
    protected final Column column;
    protected final ConfigSource config;

    protected AbstractColumnWriter(Column column, ConfigSource config)
    {
        this.column = column;
        this.config = config;
    }

    public Column getColumn()
    {
        return column;
    }
}
