/*
 * Copyright 2017 The Embulk project
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

import java.time.Instant;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
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

    @SuppressWarnings("deprecation")  // org.embulk.spi.time.Timestamp
    public Instant getTimestamp(final Column column) {
        if (HAS_GET_TIMESTAMP_INSTANT_COLUMN) {
            return this.pageReader.getTimestampInstant(column);
        } else if (HAS_GET_TIMESTAMP_COLUMN) {
            return this.pageReader.getTimestamp(column).getInstant();
        } else {
            throw new IllegalStateException(
                    "Neither PageReader#getTimestamp(Column) nor PageReader#getTimestampInstant(Column) found.");
        }
    }

    @SuppressWarnings("deprecation")  // org.embulk.spi.time.Timestamp
    public Instant getTimestamp(final int columnIndex) {
        if (HAS_GET_TIMESTAMP_INSTANT_INT) {
            return this.pageReader.getTimestampInstant(columnIndex);
        } else if (HAS_GET_TIMESTAMP_INT) {
            return this.pageReader.getTimestamp(columnIndex).getInstant();
        } else {
            throw new IllegalStateException(
                    "Neither PageReader#getTimestamp(int) nor PageReader#getTimestampInstant(int) found.");
        }
    }

    public Value getJson(final Column column) {
        return this.pageReader.getJson(column);
    }

    public Value getJson(final int columnIndex) {
        return this.pageReader.getJson(columnIndex);
    }

    private static boolean hasGetTimestampColumn() {
        try {
            PageReader.class.getMethod("getTimestamp", Column.class);
        } catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static boolean hasGetTimestampInt() {
        try {
            PageReader.class.getMethod("getTimestamp", int.class);
        } catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static boolean hasGetTimestampInstantColumn() {
        try {
            PageReader.class.getMethod("getTimestampInstant", Column.class);
        } catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static boolean hasGetTimestampInstantInt() {
        try {
            PageReader.class.getMethod("getTimestampInstant", int.class);
        } catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static final boolean HAS_GET_TIMESTAMP_COLUMN = hasGetTimestampColumn();

    private static final boolean HAS_GET_TIMESTAMP_INT = hasGetTimestampInt();

    private static final boolean HAS_GET_TIMESTAMP_INSTANT_COLUMN = hasGetTimestampInstantColumn();

    private static final boolean HAS_GET_TIMESTAMP_INSTANT_INT = hasGetTimestampInstantInt();

    private final PageReader pageReader;
}
