package org.embulk.base.restclient.record.values;

import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParser;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.base.restclient.record.ValueImporter;
import org.embulk.base.restclient.record.ValueLocator;

public class JsonValueImporter
        extends ValueImporter
{
    public JsonValueImporter(Column column, ValueLocator valueLocator, JsonParser jsonParser)
    {
        super(column, valueLocator);
        this.jsonParser = jsonParser;
    }

    @Override
    public void findAndImportValue(ServiceRecord record, PageBuilder pageBuilder)
    {
        try {
            ServiceValue value = findValue(record);
            if (value == null || value.isNull()) {
                pageBuilder.setNull(getColumnToImport());
            }
            else {
                pageBuilder.setJson(getColumnToImport(), value.jsonValue(jsonParser));
            }
        } catch (Exception ex) {
            throw new DataException("Failed to import a value for column: " + getColumnToImport().getName() + " (" + getColumnToImport().getType().getName() + ")", ex);
        }
    }

    private final JsonParser jsonParser;
}
