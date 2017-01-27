package org.embulk.base.restclient.writer;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.base.restclient.record.ValueLocator;

public abstract class ColumnWriter
{
    protected ColumnWriter(Column column, ValueLocator valueLocator)
    {
        this.column = column;
        this.valueLocator = valueLocator;
    }

    public abstract void writeColumnResponsible(ServiceRecord record, PageBuilder pageBuilderToLoad);

    protected final Column getColumnResponsible()
    {
        return column;
    }

    protected final ServiceValue pickupValueResponsible(ServiceRecord record)
    {
        return record.getValue(valueLocator);
    }

    private final Column column;
    private final ValueLocator valueLocator;
}
