package org.embulk.base.restclient.record;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;

public abstract class EmbulkValueScope
{
    protected EmbulkValueScope()
    {
        this.cachedSchema = null;
        this.cachedSingleColumn = null;
    }

    public abstract ServiceValue scopeEmbulkValues(SinglePageRecordReader singlePageRecordReader);

    protected final Column cacheSingleColumn(Schema schema, String columnName)
    {
        // Do not check the schema equality because checking schema equality needs O(N).
        if (this.cachedSchema == null) {
            this.cachedSchema = schema;
        }

        if (this.cachedSingleColumn != null) {
            if (!this.cachedSingleColumn.getName().equals(columnName)) {
                throw new SchemaConfigException("Incompatible Embulk Column: "
                                                + this.cachedSingleColumn.toString()
                                                + " : "
                                                + columnName);
            }

            // Check the schema only at the specified column.
            int cachedColumnIndex = this.cachedSingleColumn.getIndex();
            Column columnByIndex = schema.getColumn(cachedColumnIndex);
            if (!columnByIndex.equals(this.cachedSingleColumn)) {
                throw new SchemaConfigException("Incompatible Embulk Column: "
                                                + columnByIndex.toString()
                                                + " : "
                                                + columnName);
            }
            if (!columnByIndex.getName().equals(columnName)) {
                throw new SchemaConfigException("Incompatible Embulk Column: "
                                                + this.cachedSingleColumn.toString()
                                                + " : "
                                                + columnByIndex.toString()
                                                + " : "
                                                + columnName);
            }
        }
        else {
            this.cachedSingleColumn = schema.lookupColumn(columnName);
        }
        return this.cachedSingleColumn;
    }

    private Schema cachedSchema;
    private Column cachedSingleColumn;
}
