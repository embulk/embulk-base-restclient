package org.embulk.base.restclient.jackson;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.Map;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ValueImporter;
import org.embulk.base.restclient.record.values.BooleanValueImporter;
import org.embulk.base.restclient.record.values.DoubleValueImporter;
import org.embulk.base.restclient.record.values.JsonValueImporter;
import org.embulk.base.restclient.record.values.LongValueImporter;
import org.embulk.base.restclient.record.values.StringValueImporter;
import org.embulk.base.restclient.record.values.TimestampValueImporter;
import org.embulk.config.Task;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

/**
 * |JacksonServiceResponseMapper| represents how to locate values in a JSON-based response,
 * and how the values are mapped into Embulk schema.
 */
public final class JacksonServiceResponseMapper extends ServiceResponseMapper<JacksonValueLocator> {
    /**
     * Initializes |JacksonServiceSchema|.
     *
     * |JacksonServiceSchema| must be created through |JacksonServiceSchema.Builder|.
     * This constructor is private to guarantee the restriction.
     */
    protected JacksonServiceResponseMapper(final ListMultimap<Column, ColumnOptions<JacksonValueLocator>> map) {
        super(map);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public RecordImporter createRecordImporter() {
        final ImmutableList.Builder<ValueImporter> listBuilder = ImmutableList.builder();
        for (final Map.Entry<Column, ColumnOptions<JacksonValueLocator>> entry : entries()) {
            listBuilder.add(createValueImporter(entry.getKey(), entry.getValue()));
        }
        return new RecordImporter(listBuilder.build());
    }

    public static final class Builder {
        private Builder() {
            this.mapBuilder = ImmutableListMultimap.builder();
            this.index = 0;
        }

        public synchronized JacksonServiceResponseMapper.Builder add(final String embulkColumnName, final Type embulkColumnType) {
            this.mapBuilder.put(
                    new Column(index++, embulkColumnName, embulkColumnType),
                    new ColumnOptions<JacksonValueLocator>(new JacksonTopLevelValueLocator(embulkColumnName)));
            return this;
        }

        public synchronized JacksonServiceResponseMapper.Builder add(
                final String embulkColumnName,
                final Type embulkColumnType,
                final String embulkColumnTimestampFormat) {
            this.mapBuilder.put(
                    new Column(index++, embulkColumnName, embulkColumnType),
                    new ColumnOptions<JacksonValueLocator>(
                            new JacksonTopLevelValueLocator(embulkColumnName),
                            embulkColumnTimestampFormat));
            return this;
        }

        public synchronized JacksonServiceResponseMapper.Builder add(
                final JacksonValueLocator valueLocator,
                final String embulkColumnName,
                final Type embulkColumnType) {
            this.mapBuilder.put(
                    new Column(index++, embulkColumnName, embulkColumnType),
                    new ColumnOptions<JacksonValueLocator>(valueLocator));
            return this;
        }

        public synchronized JacksonServiceResponseMapper.Builder add(
                final JacksonValueLocator valueLocator,
                final String embulkColumnName,
                final Type embulkColumnType,
                final String embulkColumnTimestampFormat) {
            this.mapBuilder.put(
                    new Column(index++, embulkColumnName, embulkColumnType),
                    new ColumnOptions<JacksonValueLocator>(valueLocator, embulkColumnTimestampFormat));
            return this;
        }

        public JacksonServiceResponseMapper build() {
            return new JacksonServiceResponseMapper(mapBuilder.build());
        }

        private final ImmutableListMultimap.Builder<Column, ColumnOptions<JacksonValueLocator>> mapBuilder;
        private int index;
    }

    private interface InternalTimestampParserTask extends Task, TimestampParser.Task {
    }

    private interface InternalTimestampColumnOption extends Task, TimestampParser.TimestampColumnOption {
    }

    private ValueImporter createValueImporter(final Column column, final ColumnOptions<JacksonValueLocator> columnOptions) {
        final Type type = column.getType();
        final JacksonValueLocator locator = columnOptions.getValueLocator();
        final Optional<String> timestampFormat = columnOptions.getTimestampFormat();
        if (type.equals(Types.BOOLEAN)) {
            return new BooleanValueImporter(column, locator);
        } else if (type.equals(Types.DOUBLE)) {
            return new DoubleValueImporter(column, locator);
        } else if (type.equals(Types.LONG)) {
            return new LongValueImporter(column, locator);
        } else if (type.equals(Types.STRING)) {
            return new StringValueImporter(column, locator);
        } else if (type.equals(Types.TIMESTAMP)) {
            final TimestampParser timestampParser = new TimestampParser(
                    Exec.newConfigSource().loadConfig(InternalTimestampParserTask.class),
                    (timestampFormat.isPresent()
                         ? Exec.newConfigSource().set("format", timestampFormat.get())
                         : Exec.newConfigSource()).loadConfig(InternalTimestampColumnOption.class));
            return new TimestampValueImporter(column, locator, timestampParser);
        } else if (type.equals(Types.JSON)) {
            final JsonParser jsonParser = new JsonParser();
            return new JsonValueImporter(column, locator, jsonParser);
        } else {
            throw new IllegalStateException();
        }
    }
}
