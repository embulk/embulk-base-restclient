package org.embulk.base.restclient.record.values;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.base.restclient.record.ValueImporter;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParser;

public class JsonValueImporter extends ValueImporter {
    public JsonValueImporter(final Column column, final ValueLocator valueLocator, final JsonParser jsonParser) {
        super(column, valueLocator);
        this.jsonParser = jsonParser;
    }

    @Override
    public void findAndImportValue(final ServiceRecord record, final PageBuilder pageBuilder) {
        try {
            final ServiceValue value = findValue(record);
            if (value == null || value.isNull()) {
                pageBuilder.setNull(getColumnToImport());
            } else {
                pageBuilder.setJson(getColumnToImport(), value.jsonValue(jsonParser));
            }
        } catch (final Exception ex) {
            throw new DataException(
                    "Failed to import a value for column: " + getColumnToImport().getName()
                    + " (" + getColumnToImport().getType().getName() + ")",
                    ex);
        }
    }

    private final JsonParser jsonParser;
}
