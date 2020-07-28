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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.Collection;
import java.util.Map;
import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.RecordExporter;
import org.embulk.base.restclient.record.ValueLocator;

/**
 * |ServiceRequestMapper| represents which Embulk values are mapped into a request, and
 * how to place the values into the request.
 */
public abstract class ServiceRequestMapper<T extends ValueLocator> {
    protected ServiceRequestMapper(final ListMultimap<EmbulkValueScope, ColumnOptions<T>> map) {
        this.map = ImmutableListMultimap.copyOf(map);
    }

    public abstract RecordExporter createRecordExporter();

    protected final Collection<Map.Entry<EmbulkValueScope, ColumnOptions<T>>> entries() {
        return map.entries();
    }

    protected static class ColumnOptions<U extends ValueLocator> {
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

    private final ImmutableListMultimap<EmbulkValueScope, ColumnOptions<T>> map;
}
