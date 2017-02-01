package org.embulk.base.restclient.writer;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.base.restclient.record.ValueLocator;

public class StringColumnWriter
        extends ColumnWriter
{
    public StringColumnWriter(Column column, ValueLocator valueLocator)
    {
        super(column, valueLocator);
    }

    @Override
    public void writeColumnResponsible(ServiceRecord record, PageBuilder pageBuilder)
    {
        ServiceValue value = pickupValueResponsible(record);
        if (value == null || value.isNull()) {
            pageBuilder.setNull(getColumnResponsible());
        }
        else {
            pageBuilder.setString(getColumnResponsible(), value.stringValue());
        }
    }
}
