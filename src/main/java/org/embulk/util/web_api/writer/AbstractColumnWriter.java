package org.embulk.util.web_api.writer;

import org.embulk.config.ConfigSource;
import org.embulk.spi.Column;

import java.util.Locale;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;

abstract class AbstractColumnWriter
        implements ColumnWriter
{
    protected final Column column;
    protected final String attributeName;
    protected final ConfigSource config;

    protected AbstractColumnWriter(Column column, ConfigSource config)
    {
        this.column = column;
        this.attributeName = config.get(String.class, "attribute");
        this.config = config;
    }

    public Column getColumn()
    {
        return column;
    }

    public String getAttributeName()
    {
        return attributeName;
    }
}
