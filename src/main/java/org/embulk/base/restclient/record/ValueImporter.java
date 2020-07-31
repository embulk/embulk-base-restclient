package org.embulk.base.restclient.record;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public abstract class ValueImporter {
    protected ValueImporter(final Column column, final ValueLocator valueLocator) {
        this.column = column;
        this.valueLocator = valueLocator;
    }

    public abstract void findAndImportValue(ServiceRecord record, PageBuilder pageBuilder);

    protected final Column getColumnToImport() {
        return this.column;
    }

    protected final ServiceValue findValue(final ServiceRecord record) {
        return record.getValue(this.valueLocator);
    }

    private final Column column;
    private final ValueLocator valueLocator;
}
