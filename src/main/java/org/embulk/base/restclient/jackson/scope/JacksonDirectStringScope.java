package org.embulk.base.restclient.jackson.scope;

import org.embulk.base.restclient.record.SinglePageRecordReader;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;

public class JacksonDirectStringScope extends JacksonStringScopeBase {
    public JacksonDirectStringScope(final String embulkColumnName) {
        this.embulkColumnName = embulkColumnName;
    }

    @Override
    public String scopeString(final SinglePageRecordReader singlePageRecordReader) {
        final Schema embulkSchema = singlePageRecordReader.getSchema();
        final Column column = cacheSingleColumn(embulkSchema, this.embulkColumnName);
        return singlePageRecordReader.getString(column);
    }

    private final String embulkColumnName;
}
