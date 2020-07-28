/*
 * Copyright 2016 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
public abstract class EmbulkValueScope {
    protected EmbulkValueScope() {
        this.cachedSchema = null;
        this.cachedSingleColumn = null;
    }

    public abstract ServiceValue scopeEmbulkValues(SinglePageRecordReader singlePageRecordReader);

    protected final Column cacheSingleColumn(final Schema schema, final String columnName) {
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
            final int cachedColumnIndex = this.cachedSingleColumn.getIndex();
            final Column columnByIndex = schema.getColumn(cachedColumnIndex);
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
        } else {
            this.cachedSingleColumn = schema.lookupColumn(columnName);
        }
        return this.cachedSingleColumn;
    }

    private Schema cachedSchema;
    private Column cachedSingleColumn;
}
