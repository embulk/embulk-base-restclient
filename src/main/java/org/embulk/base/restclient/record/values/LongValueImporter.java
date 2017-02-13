package org.embulk.base.restclient.record.values;

import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.base.restclient.record.ValueImporter;
import org.embulk.base.restclient.record.ValueLocator;

public class LongValueImporter
        extends ValueImporter
{
    public LongValueImporter(Column column, ValueLocator valueLocator)
    {
        super(column, valueLocator);
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
                pageBuilder.setLong(getColumnToImport(), value.longValue());
            }
        } catch (Exception ex) {
            throw new DataException("Failed to import a value for column: " + getColumnToImport().getName() + " (" + getColumnToImport().getType().getName() + ")", ex);
        }
    }
}
