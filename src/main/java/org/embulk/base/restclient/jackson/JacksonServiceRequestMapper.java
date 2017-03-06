package org.embulk.base.restclient.jackson;

import java.util.Map;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;

import org.embulk.base.restclient.ServiceRequestMapper;
import org.embulk.base.restclient.jackson.scope.JacksonNewObjectScope;
import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.RecordExporter;
import org.embulk.base.restclient.record.ValueExporter;

/**
 * |JacksonServiceRequestMapper| represents which Embulk values are mapped into JSON, and
 * how the values are placed in a JSON-based request.
 */
public final class JacksonServiceRequestMapper
        extends ServiceRequestMapper<JacksonValueLocator>
{
    protected JacksonServiceRequestMapper(
            ListMultimap<EmbulkValueScope, ColumnOptions<JacksonValueLocator>> map)
    {
        super(map);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public RecordExporter createRecordExporter()
    {
        ImmutableList.Builder<ValueExporter> listBuilder = ImmutableList.builder();
        for (Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>> entry : entries()) {
            listBuilder.add(createValueExporter(entry.getKey(), entry.getValue()));
        }
        return new RecordExporter(listBuilder.build(), JacksonServiceRecord.builder());
    }

    public static final class Builder
    {
        private Builder()
        {
            this.mapBuilder = ImmutableListMultimap.builder();
        }

        public synchronized JacksonServiceRequestMapper.Builder addNewObject(
                JacksonValueLocator valueLocator)
        {
            mapBuilder.put(new JacksonNewObjectScope(),
                           new ColumnOptions<JacksonValueLocator>(valueLocator));
            return this;

        }

        public synchronized JacksonServiceRequestMapper.Builder add(
                EmbulkValueScope scope,
                JacksonValueLocator valueLocator)
        {
            mapBuilder.put(scope,
                           new ColumnOptions<JacksonValueLocator>(
                               valueLocator));
            return this;

        }

        public JacksonServiceRequestMapper build()
        {
            return new JacksonServiceRequestMapper(mapBuilder.build());
        }

        private final ImmutableListMultimap.Builder<EmbulkValueScope, ColumnOptions<JacksonValueLocator>> mapBuilder;
    }

    private ValueExporter createValueExporter(EmbulkValueScope embulkValueScope,
                                              ColumnOptions<JacksonValueLocator> columnOptions)
    {
        JacksonValueLocator locator = columnOptions.getValueLocator();
        return new ValueExporter(embulkValueScope, locator);
    }
}
