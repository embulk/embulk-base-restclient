package org.embulk.input.web_api.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.input.web_api.WebApiPluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class SchemaWriter<PluginTask extends WebApiPluginTask>
{
    private ColumnWriter[] writers;

    public SchemaWriter()
    {
    }

    public SchemaWriter buildColumnWriters(PluginTask task, Schema schema)
    {
        this.writers = new ColumnWriter[schema.size()];
        for (int i = 0; i < writers.length; i++) {
            Column column = schema.getColumn(i);
            Type type = column.getType();
            if (type.equals(Types.BOOLEAN)) {
                writers[i] = buildBooleanColumnWriter(task, column);
            }
            else if (type.equals(Types.DOUBLE)) {
                writers[i] = buildDoubleColumnWriter(task, column);
            }
            else if (type.equals(Types.LONG)) {
                writers[i] = buildLongColumnWriter(task, column);
            }
            else if (type.equals(Types.STRING)) {
                writers[i] = buildStringColumnWriter(task, column);
            }
            else if (type.equals(Types.TIMESTAMP)) {
                writers[i] = buildTimestampColumnWriter(task, column);
            }
            else if (type.equals(Types.JSON)) {
                writers[i] = buildJsonColumnWriter(task, column);
            }
            else {
                throw new IllegalStateException();
            }
        }
        return this;
    }

    protected ColumnWriter buildBooleanColumnWriter(PluginTask task, Column column)
    {
        ColumnWriter writer = new BooleanColumnWriter(column);
        writers[column.getIndex()] = writer;
        return writer;
    }

    protected ColumnWriter buildDoubleColumnWriter(PluginTask task, Column column)
    {
        ColumnWriter writer = new DoubleColumnWriter(column);
        writers[column.getIndex()] = writer;
        return writer;
    }

    protected ColumnWriter buildLongColumnWriter(PluginTask task, Column column)
    {
        ColumnWriter writer = new LongColumnWriter(column);
        writers[column.getIndex()] = writer;
        return writer;
    }

    protected ColumnWriter buildStringColumnWriter(PluginTask task, Column column)
    {
        ColumnWriter writer = new StringColumnWriter(column);
        writers[column.getIndex()] = writer;
        return writer;
    }

    protected ColumnWriter buildTimestampColumnWriter(PluginTask task, Column column)
    {
        ColumnWriter writer = new TimestampColumnWriter(column, new TimestampParser(task, task));
        writers[column.getIndex()] = writer;
        return writer;
    }

    protected ColumnWriter buildJsonColumnWriter(PluginTask task, Column column)
    {
        ColumnWriter writer = new JsonColumnWriter(column, new JsonParser());
        writers[column.getIndex()] = writer;
        return writer;
    }

    public void write(JsonNode record, PageBuilder to)
    {
        for (ColumnWriter w : writers) {
            Column column = w.getColumn();
            w.write(record.get(column.getName()), to);
        }
    }
}
