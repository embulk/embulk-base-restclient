package org.embulk.base.restclient.record;

import java.util.List;
import org.embulk.spi.PageBuilder;

public class RecordImporter {
    public RecordImporter(final List<ValueImporter> valueImporters) {
        this.valueImporters = valueImporters;
    }

    public void importRecord(final ServiceRecord record, final PageBuilder pageBuilder) {
        for (final ValueImporter valueImporter : this.valueImporters) {
            valueImporter.findAndImportValue(record, pageBuilder);
        }
        pageBuilder.addRecord();
    }

    private List<ValueImporter> valueImporters;
}
