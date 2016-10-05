package org.embulk.util.web_api.schema;

import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigSource;
import org.embulk.util.web_api.writer.BooleanColumnWriter;
import org.embulk.util.web_api.writer.ColumnWriter;
import org.embulk.util.web_api.writer.DoubleColumnWriter;
import org.embulk.util.web_api.writer.JsonColumnWriter;
import org.embulk.util.web_api.writer.LongColumnWriter;
import org.embulk.util.web_api.writer.SchemaWriter;
import org.embulk.util.web_api.writer.StringColumnWriter;
import org.embulk.util.web_api.writer.TimestampColumnWriter;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import javax.validation.constraints.NotNull;

import java.util.List;

import static org.embulk.spi.Exec.newConfigSource;

public class SchemaWrapper
{
    private final List<Column> columns;
    private final List<ColumnWriter> writers;

    public SchemaWrapper(List<Column> columns, List<ColumnWriter> writers)
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

    public static class Builder
    {
        private final ImmutableList.Builder<Column> columns = ImmutableList.builder();
        private final ImmutableList.Builder<ColumnWriter> writers = ImmutableList.builder();

        private int index = 0;

        public Builder()
        {
        }

        public SchemaWrapper.Builder add(String name, Type type)
        {
            return add(name, type, newConfigSource());
        }

        public synchronized SchemaWrapper.Builder add(String name, Type type, @NotNull ConfigSource config)
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

        private SchemaWrapper.Builder self()
        {
            return this;
        }

        public SchemaWrapper build()
        {
            return new SchemaWrapper(columns.build(), writers.build());
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
