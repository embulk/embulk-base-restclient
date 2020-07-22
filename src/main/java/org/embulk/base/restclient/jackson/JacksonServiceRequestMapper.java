package org.embulk.base.restclient.jackson;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
public final class JacksonServiceRequestMapper
        extends ServiceRequestMapper<JacksonValueLocator>
{
    protected JacksonServiceRequestMapper(
            List<Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>>> map)
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
        final ArrayList<ValueExporter> listBuilder = new ArrayList<>();
        for (Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>> entry : entries()) {
            listBuilder.add(createValueExporter(entry.getKey(), entry.getValue()));
        }
        return new RecordExporter(Collections.unmodifiableList(listBuilder), JacksonServiceRecord.builder());
    }

    public static final class Builder
    {
        private Builder()
        {
            this.mapBuilder = new ArrayList<>();
        }

        public synchronized JacksonServiceRequestMapper.Builder addNewObject(
                JacksonValueLocator valueLocator)
        {
            mapBuilder.add(new AbstractMap.SimpleEntry<>(
                    new JacksonNewObjectScope(),
                    new ColumnOptions<JacksonValueLocator>(valueLocator)));
            return this;

        }

        public synchronized JacksonServiceRequestMapper.Builder add(
                EmbulkValueScope scope,
                JacksonValueLocator valueLocator)
        {
            mapBuilder.add(new AbstractMap.SimpleEntry<>(
                    scope,
                    new ColumnOptions<JacksonValueLocator>(valueLocator)));
            return this;

        }

        public JacksonServiceRequestMapper build()
        {
            final ArrayList<Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>>> built = new ArrayList<>();
            for (final Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>> entry : this.mapBuilder) {
                built.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
            }
            return new JacksonServiceRequestMapper(Collections.unmodifiableList(mapBuilder));
        }

        private final ArrayList<Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>>> mapBuilder;
    }

    private ValueExporter createValueExporter(EmbulkValueScope embulkValueScope,
                                              ColumnOptions<JacksonValueLocator> columnOptions)
    {
        JacksonValueLocator locator = columnOptions.getValueLocator();
        return new ValueExporter(embulkValueScope, locator);
    }
}
