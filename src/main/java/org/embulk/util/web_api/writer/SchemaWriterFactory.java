package org.embulk.util.web_api.writer;

import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import javax.validation.constraints.NotNull;

import java.util.List;

import static org.embulk.spi.Exec.newConfigSource;

public class SchemaWriterFactory
{
    private final List<Column> columns;
    private final List<ColumnWriter> writers;

    public SchemaWriterFactory(List<Column> columns, List<ColumnWriter> writers)
    {
        this.columns = columns;
        this.writers = writers;
    }

    public Schema newSchema()
    {
        return new Schema(columns);
    }

    public SchemaWriter newSchemaWriter()
    {
        return new SchemaWriter(writers);
    }

    public static SchemaWriterFactory.Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableList.Builder<Column> columns = ImmutableList.builder();
        private final ImmutableList.Builder<ColumnWriter> writers = ImmutableList.builder();

        private int index = 0;

        public SchemaWriterFactory.Builder add(String name, Type type)
        {
            return add(name, type, newConfigSource());
        }

        public synchronized SchemaWriterFactory.Builder add(String name, Type type, @NotNull ConfigSource config)
        {
            Column column = new Column(index++, name, type);
            columns.add(column);
            writers.add(buildColumnWriter(column, config));
            return self();
        }

        private ColumnWriter buildColumnWriter(Column column, ConfigSource config)
        {
            Type type = column.getType();
            if (type.equals(Types.BOOLEAN)) {
                return new BooleanColumnWriter(column, config);
            }
            else if (type.equals(Types.DOUBLE)) {
                return new DoubleColumnWriter(column, config);
            }
            else if (type.equals(Types.LONG)) {
                return new LongColumnWriter(column, config);
            }
            else if (type.equals(Types.STRING)) {
                return new StringColumnWriter(column, config);
            }
            else if (type.equals(Types.TIMESTAMP)) {
                TimestampParser timestampParser = new TimestampParser(
                        config.loadConfig(Task.class),
                        config.loadConfig(TimestampColumnOption.class));
                return new TimestampColumnWriter(column, config, timestampParser);

            }
            else if (type.equals(Types.JSON)) {
                JsonParser jsonParser = new JsonParser();
                return new JsonColumnWriter(column, config, jsonParser);
            }
            else {
                throw new IllegalStateException();
            }
        }

        private SchemaWriterFactory.Builder self()
        {
            return this;
        }

        public SchemaWriterFactory build()
        {
            return new SchemaWriterFactory(columns.build(), writers.build());
        }

        interface Task
                extends org.embulk.config.Task, TimestampParser.Task
        {
        }

        interface TimestampColumnOption
                extends org.embulk.config.Task, TimestampParser.TimestampColumnOption
        {
        }
    }
}
