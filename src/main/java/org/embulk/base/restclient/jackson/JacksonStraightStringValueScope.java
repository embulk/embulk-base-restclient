package org.embulk.base.restclient.jackson;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;

import org.embulk.base.restclient.record.SinglePageRecordReader;

public class JacksonStraightStringValueScope
        extends JacksonStringValueScope
{
    public JacksonStraightStringValueScope(String embulkColumnName)
    {
        this.embulkColumnName = embulkColumnName;
    }

    @Override
    public String scopeStringValue(SinglePageRecordReader singlePageRecordReader)
    {
        Schema embulkSchema = singlePageRecordReader.getSchema();
        Column column = cacheSingleColumn(embulkSchema, this.embulkColumnName);
        return singlePageRecordReader.getString(column);
    }

    private final String embulkColumnName;
}
