package org.embulk.base.restclient.record;

import java.util.List;

import org.embulk.spi.PageBuilder;

public class RecordImporter
{
    public RecordImporter(List<ValueImporter> valueImporters)
    {
        this.valueImporters = valueImporters;
    }

    public void importRecord(ServiceRecord record, PageBuilder pageBuilder)
    {
        for (ValueImporter valueImporter : valueImporters) {
            valueImporter.findAndImportValue(record, pageBuilder);
        }
        pageBuilder.addRecord();
    }

    private List<ValueImporter> valueImporters;
}
