package org.embulk.base.restclient.writer;

import org.embulk.base.restclient.writer.SchemaWriterFactory.WebApiColumnOption;
import org.embulk.spi.Column;

abstract class AbstractColumnWriter
        implements ColumnWriter
{
    protected final Column column;
    protected final String attributeName;
    protected final WebApiColumnOption option;

    protected AbstractColumnWriter(Column column, WebApiColumnOption option)
    {
        this.column = column;
        this.attributeName = option.getAttributeName();
        this.option = option;
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
