package org.embulk.base.restclient;

import java.util.Collection;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.RecordExporter;
import org.embulk.base.restclient.record.ValueLocator;

/**
 * |ServiceRequestMapper| represents which Embulk values are mapped into a request, and
 * how to place the values into the request.
 */
abstract public class ServiceRequestMapper<T extends ValueLocator>
{
    protected ServiceRequestMapper(ListMultimap<EmbulkValueScope, ColumnOptions<T>> map)
    {
        this.map = ImmutableListMultimap.copyOf(map);
    }

    abstract public RecordExporter createRecordExporter();

    protected final Collection<Map.Entry<EmbulkValueScope, ColumnOptions<T>>> entries()
    {
        return map.entries();
    }

    protected static class ColumnOptions<U extends ValueLocator>
    {
        public ColumnOptions(U valueLocator)
        {
            this.valueLocator = valueLocator;
            this.timestampFormat = Optional.absent();
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

    private final ImmutableListMultimap<EmbulkValueScope, ColumnOptions<T>> map;
}
