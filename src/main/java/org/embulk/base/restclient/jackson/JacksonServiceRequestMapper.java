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

package org.embulk.base.restclient.jackson;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.Map;
import org.embulk.base.restclient.ServiceRequestMapper;
import org.embulk.base.restclient.jackson.scope.JacksonNewObjectScope;
import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.RecordExporter;
import org.embulk.base.restclient.record.ValueExporter;

/**
 * |JacksonServiceRequestMapper| represents which Embulk values are mapped into JSON, and
 * how the values are placed in a JSON-based request.
 */
public final class JacksonServiceRequestMapper extends ServiceRequestMapper<JacksonValueLocator> {
    protected JacksonServiceRequestMapper(final ListMultimap<EmbulkValueScope, ColumnOptions<JacksonValueLocator>> map) {
        super(map);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public RecordExporter createRecordExporter() {
        final ImmutableList.Builder<ValueExporter> listBuilder = ImmutableList.builder();
        for (final Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>> entry : entries()) {
            listBuilder.add(createValueExporter(entry.getKey(), entry.getValue()));
        }
        return new RecordExporter(listBuilder.build(), JacksonServiceRecord.builder());
    }

    public static final class Builder {
        private Builder() {
            this.mapBuilder = ImmutableListMultimap.builder();
        }

        public synchronized JacksonServiceRequestMapper.Builder addNewObject(final JacksonValueLocator valueLocator) {
            this.mapBuilder.put(
                    new JacksonNewObjectScope(),
                    new ColumnOptions<JacksonValueLocator>(valueLocator));
            return this;

        }

        public synchronized JacksonServiceRequestMapper.Builder add(
                final EmbulkValueScope scope,
                final JacksonValueLocator valueLocator) {
            this.mapBuilder.put(scope, new ColumnOptions<JacksonValueLocator>(valueLocator));
            return this;

        }

        public JacksonServiceRequestMapper build() {
            return new JacksonServiceRequestMapper(mapBuilder.build());
        }

        private final ImmutableListMultimap.Builder<EmbulkValueScope, ColumnOptions<JacksonValueLocator>> mapBuilder;
    }

    private ValueExporter createValueExporter(
            final EmbulkValueScope embulkValueScope, final ColumnOptions<JacksonValueLocator> columnOptions) {
        final JacksonValueLocator locator = columnOptions.getValueLocator();
        return new ValueExporter(embulkValueScope, locator);
    }
}
