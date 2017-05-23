package org.embulk.base.restclient.record;

import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;

import org.msgpack.value.Value;

/**
 * SinglePageRecordReader is a {@code PageReader} wrapper which ensures that {@code nextRecord} is not called.
 */
public class SinglePageRecordReader
{
    public SinglePageRecordReader(PageReader pageReader)
    {
        this.pageReader = pageReader;
    }

    public Schema getSchema()
    {
        return this.pageReader.getSchema();
    }

    public boolean isNull(Column column)
    {
        return this.pageReader.isNull(column);
    }

    public boolean isNull(int columnIndex)
    {
        return this.pageReader.isNull(columnIndex);
    }

    public boolean getBoolean(Column column)
    {
        return this.pageReader.getBoolean(column);
    }

    public boolean getBoolean(int columnIndex)
    {
        return this.pageReader.getBoolean(columnIndex);
    }

    public long getLong(Column column)
    {
        return this.pageReader.getLong(column);
    }

    public long getLong(int columnIndex)
    {
        return this.pageReader.getLong(columnIndex);
    }

    public double getDouble(Column column)
    {
        return this.pageReader.getDouble(column);
    }

    public double getDouble(int columnIndex)
    {
        return this.pageReader.getDouble(columnIndex);
    }

    public String getString(Column column)
    {
        return isNull(column) ? "" : this.pageReader.getString(column);
    }

    public String getString(int columnIndex)
    {
        return this.pageReader.getString(columnIndex);
    }

    public Timestamp getTimestamp(Column column)
    {
        return this.pageReader.getTimestamp(column);
    }

    public Timestamp getTimestamp(int columnIndex)
    {
        return this.pageReader.getTimestamp(columnIndex);
    }

    public Value getJson(Column column)
    {
        return this.pageReader.getJson(column);
    }

    public Value getJson(int columnIndex)
    {
        return this.pageReader.getJson(columnIndex);
    }

    private final PageReader pageReader;
}
