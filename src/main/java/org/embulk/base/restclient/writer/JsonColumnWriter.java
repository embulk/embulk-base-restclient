package org.embulk.base.restclient.writer;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParser;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.base.restclient.record.ValueLocator;

public class JsonColumnWriter<T extends ValueLocator>
        extends ColumnWriter<T>
{
    public JsonColumnWriter(Column column, T valueLocator, JsonParser jsonParser)
    {
        super(column, valueLocator);
        this.jsonParser = jsonParser;
    }

    @Override
    public void writeColumnResponsible(ServiceRecord<T> record, PageBuilder pageBuilderToLoad)
    {
        ServiceValue value = pickupValueResponsible(record);
        if (value == null || value.isNull()) {
            pageBuilderToLoad.setNull(getColumnResponsible());
        }
        else {
            pageBuilderToLoad.setJson(getColumnResponsible(), value.jsonValue(jsonParser));
        }
    }

    private final JsonParser jsonParser;
}
