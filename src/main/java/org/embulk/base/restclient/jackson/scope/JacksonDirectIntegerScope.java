package org.embulk.base.restclient.jackson.scope;

import org.embulk.base.restclient.record.SinglePageRecordReader;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;

public class JacksonDirectIntegerScope extends JacksonIntegerScopeBase {
    public JacksonDirectIntegerScope(final String embulkColumnName) {
        this.embulkColumnName = embulkColumnName;
    }

    @Override
    public long scopeInteger(final SinglePageRecordReader singlePageRecordReader) {
        final Schema embulkSchema = singlePageRecordReader.getSchema();
        final Column column = cacheSingleColumn(embulkSchema, this.embulkColumnName);
        return singlePageRecordReader.getLong(column);
    }

    private final String embulkColumnName;
}
