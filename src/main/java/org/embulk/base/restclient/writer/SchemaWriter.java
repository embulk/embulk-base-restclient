package org.embulk.base.restclient.writer;

import java.util.List;

import org.embulk.spi.PageBuilder;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;

public class SchemaWriter
{
    public SchemaWriter(List<ColumnWriter> columnWriters)
    {
        this.columnWriters = columnWriters;
    }

    public void addRecordTo(ServiceRecord record, PageBuilder pageBuilder)
    {
        for (ColumnWriter columnWriter : columnWriters) {
            columnWriter.writeColumnResponsible(record, pageBuilder);
        }
        pageBuilder.addRecord();
    }

    private List<ColumnWriter> columnWriters;
}
