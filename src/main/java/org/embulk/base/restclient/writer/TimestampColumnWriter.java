package org.embulk.base.restclient.writer;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParser;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.base.restclient.record.ValueLocator;

public class TimestampColumnWriter<T extends ValueLocator>
        extends ColumnWriter<T>
{
    public TimestampColumnWriter(Column column, T valueLocator, TimestampParser timestampParser)
    {
        super(column, valueLocator);
        this.timestampParser = timestampParser;
    }

    @Override
    public void writeColumnResponsible(ServiceRecord<T> record, PageBuilder pageBuilderToLoad)
    {
        ServiceValue value = pickupValueResponsible(record);
        if (value == null || value.isNull()) {
            pageBuilderToLoad.setNull(getColumnResponsible());
        }
        else {
            pageBuilderToLoad.setTimestamp(getColumnResponsible(), value.timestampValue(timestampParser));
        }
    }

    private final TimestampParser timestampParser;
}
