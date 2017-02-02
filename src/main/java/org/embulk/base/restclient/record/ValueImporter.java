package org.embulk.base.restclient.record;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public abstract class ValueImporter
{
    protected ValueImporter(Column column, ValueLocator valueLocator)
    {
        this.column = column;
        this.valueLocator = valueLocator;
    }

    public abstract void findAndImportValue(ServiceRecord record, PageBuilder pageBuilder);

    protected final Column getColumnToImport()
    {
        return column;
    }

    protected final ServiceValue findValue(ServiceRecord record)
    {
        return record.getValue(valueLocator);
    }

    private final Column column;
    private final ValueLocator valueLocator;
}
