package org.embulk.input.web_api.writer;

import org.embulk.spi.Column;

abstract class AbstractColumnWriter
        implements ColumnWriter
{
    protected final Column column;

    protected AbstractColumnWriter(Column column)
    {
        this.column = column;
    }

    public Column getColumn()
    {
        return column;
    }
}
