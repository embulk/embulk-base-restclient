package org.embulk.util.web_api.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public interface ColumnWriter
{
    Column getColumn();

    String getAttributeName();

    void write(JsonNode v, PageBuilder to);
}
