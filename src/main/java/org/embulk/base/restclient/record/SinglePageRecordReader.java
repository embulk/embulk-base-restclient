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
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

/**
 * SinglePageRecordReader is a {@code PageReader} wrapper which ensures that {@code nextRecord} is not called.
 */
public class SinglePageRecordReader {
    public SinglePageRecordReader(final PageReader pageReader) {
        this.pageReader = pageReader;
    }

    public Schema getSchema() {
        return this.pageReader.getSchema();
    }

    public boolean isNull(final Column column) {
        return this.pageReader.isNull(column);
    }

    public boolean isNull(final int columnIndex) {
        return this.pageReader.isNull(columnIndex);
    }

    public boolean getBoolean(final Column column) {
        return this.pageReader.getBoolean(column);
    }

    public boolean getBoolean(final int columnIndex) {
        return this.pageReader.getBoolean(columnIndex);
    }

    public long getLong(final Column column) {
        return this.pageReader.getLong(column);
    }

    public long getLong(final int columnIndex) {
        return this.pageReader.getLong(columnIndex);
    }

    public double getDouble(final Column column) {
        return this.pageReader.getDouble(column);
    }

    public double getDouble(final int columnIndex) {
        return this.pageReader.getDouble(columnIndex);
    }

    public String getString(final Column column) {
        return this.pageReader.getString(column);
    }

    public String getString(final int columnIndex) {
        return this.pageReader.getString(columnIndex);
    }

    public Timestamp getTimestamp(final Column column) {
        return this.pageReader.getTimestamp(column);
    }

    public Timestamp getTimestamp(final int columnIndex) {
        return this.pageReader.getTimestamp(columnIndex);
    }

    public Value getJson(final Column column) {
        return this.pageReader.getJson(column);
    }

    public Value getJson(final int columnIndex) {
        return this.pageReader.getJson(columnIndex);
    }

    private final PageReader pageReader;
}
