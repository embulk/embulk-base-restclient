package org.embulk.base.restclient;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.RecordExporter;
import org.embulk.base.restclient.record.ValueLocator;

/**
 * |ServiceRequestMapper| represents which Embulk values are mapped into a request, and
 * how to place the values into the request.
 */
abstract public class ServiceRequestMapper<T extends ValueLocator>
{
    protected ServiceRequestMapper(final List<Map.Entry<EmbulkValueScope, ColumnOptions<T>>> map)
    {
        final ArrayList<Map.Entry<EmbulkValueScope, ColumnOptions<T>>> built = new ArrayList<>();
        for (final Map.Entry<EmbulkValueScope, ColumnOptions<T>> entry : map) {
            built.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
        }
        this.map = Collections.unmodifiableList(built);
    }

    abstract public RecordExporter createRecordExporter();

    protected final Collection<Map.Entry<EmbulkValueScope, ColumnOptions<T>>> entries()
    {
        return this.map;
    }

    protected static class ColumnOptions<U extends ValueLocator>
    {
        public ColumnOptions(U valueLocator)
        {
            this.valueLocator = valueLocator;
            this.timestampFormat = Optional.empty();
        }

        public ColumnOptions(U valueLocator, String timestampFormat)
        {
            this.valueLocator = valueLocator;
            this.timestampFormat = Optional.of(timestampFormat);
        }

        public U getValueLocator()
        {
            return this.valueLocator;
        }

        public Optional<String> getTimestampFormat()
        {
            return this.timestampFormat;
        }

        private final U valueLocator;
        private final Optional<String> timestampFormat;
    }

    private final List<Map.Entry<EmbulkValueScope, ColumnOptions<T>>> map;
}
