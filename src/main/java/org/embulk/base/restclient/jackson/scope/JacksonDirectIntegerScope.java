package org.embulk.base.restclient.jackson.scope;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;

import org.embulk.base.restclient.record.SinglePageRecordReader;

public class JacksonDirectIntegerScope
        extends JacksonIntegerScopeBase
{
    public JacksonDirectIntegerScope(String embulkColumnName)
    {
        this.embulkColumnName = embulkColumnName;
    }

    @Override
    public long scopeInteger(SinglePageRecordReader singlePageRecordReader)
    {
        Schema embulkSchema = singlePageRecordReader.getSchema();
        Column column = cacheSingleColumn(embulkSchema, this.embulkColumnName);
        return singlePageRecordReader.getLong(column);
    }

    private final String embulkColumnName;
}
