package org.embulk.base.restclient.writer;

import java.util.List;

import org.embulk.spi.PageBuilder;

import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;

public class SchemaWriter<T extends ValueLocator>
{
    public SchemaWriter(List<ColumnWriter<T>> columnWriters)
    {
        this.columnWriters = columnWriters;
    }

    public void addRecordTo(ServiceRecord<T> record, PageBuilder pageBuilderToLoad)
    {
        for (ColumnWriter<T> columnWriter : columnWriters) {
            columnWriter.writeColumnResponsible(record, pageBuilderToLoad);
        }
        pageBuilderToLoad.addRecord();
    }

    private List<ColumnWriter<T>> columnWriters;
}
