package org.embulk.base.restclient.writer;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParser;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.base.restclient.record.ValueLocator;

public class JsonColumnWriter
        extends ColumnWriter
{
    public JsonColumnWriter(Column column, ValueLocator valueLocator, JsonParser jsonParser)
    {
        super(column, valueLocator);
        this.jsonParser = jsonParser;
    }

    @Override
    public void writeColumnResponsible(ServiceRecord record, PageBuilder pageBuilder)
    {
        ServiceValue value = pickupValueResponsible(record);
        if (value == null || value.isNull()) {
            pageBuilder.setNull(getColumnResponsible());
        }
        else {
            pageBuilder.setJson(getColumnResponsible(), value.jsonValue(jsonParser));
        }
    }

    private final JsonParser jsonParser;
}
