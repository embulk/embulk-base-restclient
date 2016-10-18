package org.embulk.util.web_api.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.util.web_api.WebApiPluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

import java.util.List;

public class SchemaWriter<PluginTask extends WebApiPluginTask>
{
    private List<ColumnWriter> writers;

    public SchemaWriter(List<ColumnWriter> writers)
    {
        this.writers = writers;
    }

    public void write(JsonNode record, PageBuilder to)
    {
        for (ColumnWriter w : writers) {
            Column column = w.getColumn();
            w.write(record.get(column.getName()), to);
        }
    }
}
