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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.Collection;
import java.util.Map;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;

/**
 * |ServiceResponseMapper| represents how to locate values in a response, and how the values are
 * mapped into Embulk schema.
 */
public abstract class ServiceResponseMapper<T extends ValueLocator> {
    protected ServiceResponseMapper(final ListMultimap<Column, ColumnOptions<T>> map) {
        this.map = ImmutableListMultimap.copyOf(map);
    }

    public final Schema getEmbulkSchema() {
        // Assume ListMultimap#keys is ordered.
        return new Schema(ImmutableList.copyOf(map.keys()));
    }

    public abstract RecordImporter createRecordImporter();

    protected final Collection<Map.Entry<Column, ColumnOptions<T>>> entries() {
        return map.entries();
    }

    public static class ColumnOptions<U extends ValueLocator> {
        public ColumnOptions(final U valueLocator) {
            this.valueLocator = valueLocator;
            this.timestampFormat = Optional.absent();
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

    private final ImmutableListMultimap<Column, ColumnOptions<T>> map;
}
