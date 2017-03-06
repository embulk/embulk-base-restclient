package org.embulk.base.restclient.record;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;

/**
 * EmbulkValueScope defines how to pick up (scope) a single value from an entire Embulk record.
 *
 * The most simple cases are to pick up an Embulk value as a single value as-is, for example:
 * - Pick up an Embulk String value as a string value.
 * - Pick up an Embulk Long (interger) value as an integer value.
 *
 * There can be more complicated cases, unfortunately. For example, the destination may need
 * a different type.
 * - Pick up an Embulk String value, and uses its length as an integer value.
 *
 * Or, the destination may need a value which are generated from multiple Embulk values.
 * - Pick up an Embulk String S and an Integer N, and uses a string S truncated to the length N.
 *
 * To satisfy such possible requirements, {@code EmbulkValueScope#scopeEmbulkValues}
 * 1) inputs an entire Embulk record through {@code SinglePageRecordReader},
 * 2) picks up arbitrary values from the record, and
 * 3) returns {@code ServiceValue} which contains any single value.
 */
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
