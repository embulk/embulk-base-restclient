package org.embulk.base.restclient.jackson.scope;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;

import org.embulk.base.restclient.record.SinglePageRecordReader;

public class JacksonDirectStringScope
        extends JacksonStringScopeBase
{
    public JacksonDirectStringScope(String embulkColumnName)
    {
        this.embulkColumnName = embulkColumnName;
    }

    @Override
    public String scopeString(SinglePageRecordReader singlePageRecordReader)
    {
        Schema embulkSchema = singlePageRecordReader.getSchema();
        Column column = cacheSingleColumn(embulkSchema, this.embulkColumnName);
        return singlePageRecordReader.getString(column);
    }

    private final String embulkColumnName;
}
