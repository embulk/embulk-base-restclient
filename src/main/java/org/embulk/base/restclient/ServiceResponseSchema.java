package org.embulk.base.restclient;

import java.util.Collection;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;

import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.base.restclient.writer.SchemaWriter;

/**
 * |ServiceResponseSchema| represents how to locate values in a response, and how the values are
 * mapped into Embulk schema.
 */
public abstract class ServiceResponseSchema<T extends ValueLocator>
{
    protected ServiceResponseSchema(ListMultimap<Column, ColumnOptions<T>> map)
    {
        this.map = ImmutableListMultimap.copyOf(map);
    }

    public final Schema getEmbulkSchema()
    {
        // Assume ListMultimap#keys is ordered.
        return new Schema(ImmutableList.copyOf(map.keys()));
    }

    public abstract SchemaWriter<T> createSchemaWriter();

    protected final Collection<Map.Entry<Column, ColumnOptions<T>>> entries()
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

    private final ImmutableListMultimap<Column, ColumnOptions<T>> map;
}
