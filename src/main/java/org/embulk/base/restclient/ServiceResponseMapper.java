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

package org.embulk.base.restclient;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;

/**
 * |ServiceResponseMapper| represents how to locate values in a response, and how the values are
 * mapped into Embulk schema.
 */
public abstract class ServiceResponseMapper<T extends ValueLocator> {
    protected ServiceResponseMapper(final List<Map.Entry<Column, ColumnOptions<T>>> map) {
        final ArrayList<Map.Entry<Column, ColumnOptions<T>>> built = new ArrayList<>();
        for (final Map.Entry<Column, ColumnOptions<T>> entry : map) {
            built.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
        }
        this.map = Collections.unmodifiableList(built);
    }

    public final Schema getEmbulkSchema() {
        final ArrayList<Column> built = new ArrayList<>();
        for (final Map.Entry<Column, ColumnOptions<T>> entry : this.map) {
            built.add(entry.getKey());
        }
        return new Schema(built);
    }

    public abstract RecordImporter createRecordImporter();

    protected final Collection<Map.Entry<Column, ColumnOptions<T>>> entries() {
        return this.map;
    }

    public static class ColumnOptions<U extends ValueLocator> {
        public ColumnOptions(final U valueLocator) {
            this.valueLocator = valueLocator;
            this.timestampFormat = Optional.empty();
        }

        public ColumnOptions(final U valueLocator, final String timestampFormat) {
            this.valueLocator = valueLocator;
            this.timestampFormat = Optional.of(timestampFormat);
        }

        public U getValueLocator() {
            return this.valueLocator;
        }

        public Optional<String> getTimestampFormat() {
            return this.timestampFormat;
        }

        private final U valueLocator;
        private final Optional<String> timestampFormat;
    }

    private final List<Map.Entry<Column, ColumnOptions<T>>> map;
}
