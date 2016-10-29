package org.embulk.util.web_api.writer;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
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
    interface WebApiColumnOption
            extends org.embulk.config.Task, TimestampParser.Task, TimestampParser.TimestampColumnOption
    {
        @Config("column_name")
        @ConfigDefault("null")
        public Optional<String> getColumnName();

        public void setAttributeName(String name);
        public String getAttributeName();
    }

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
            WebApiColumnOption option = buildOption(name, config);
            TaskSource task = option.dump();

            Column column = buildColumn(index++, name, type, task);
            columns.add(column);
            writers.add(buildColumnWriter(column, task));
            return self();
        }

        private WebApiColumnOption buildOption(String name, ConfigSource config)
        {
            WebApiColumnOption option = config.loadConfig(WebApiColumnOption.class);
            option.setAttributeName(name);
            return option;
        }

        private Column buildColumn(int index, String name, Type type, TaskSource task)
        {
            WebApiColumnOption option = task.loadTask(WebApiColumnOption.class);
            // column_name
            if (option.getColumnName().isPresent()) {
                name = option.getColumnName().get();
            }
            return new Column(index, name, type);
        }

        private ColumnWriter buildColumnWriter(Column column, TaskSource task)
        {
            WebApiColumnOption option = task.loadTask(WebApiColumnOption.class);
            Type type = column.getType();
            if (type.equals(Types.BOOLEAN)) {
                return new BooleanColumnWriter(column, option);
            }
            else if (type.equals(Types.DOUBLE)) {
                return new DoubleColumnWriter(column, option);
            }
            else if (type.equals(Types.LONG)) {
                return new LongColumnWriter(column, option);
            }
            else if (type.equals(Types.STRING)) {
                return new StringColumnWriter(column, option);
            }
            else if (type.equals(Types.TIMESTAMP)) {
                TimestampParser timestampParser = new TimestampParser(
                        task.loadTask(Task.class),
                        task.loadTask(WebApiColumnOption.class));
                return new TimestampColumnWriter(column, option, timestampParser);

            }
            else if (type.equals(Types.JSON)) {
                JsonParser jsonParser = new JsonParser();
                return new JsonColumnWriter(column, option, jsonParser);
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
    }
}
