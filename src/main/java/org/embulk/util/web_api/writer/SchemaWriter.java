package org.embulk.util.web_api.writer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.spi.PageBuilder;

import java.util.List;

public class SchemaWriter
{
    private List<ColumnWriter> writers;

    public SchemaWriter(List<ColumnWriter> writers)
    {
        this.writers = writers;
    }

    public void addRecordTo(ObjectNode record, PageBuilder to)
    {
        for (ColumnWriter w : writers) {
            w.write(record.get(w.getAttributeName()), to);
        }
        to.addRecord();
    }
}
