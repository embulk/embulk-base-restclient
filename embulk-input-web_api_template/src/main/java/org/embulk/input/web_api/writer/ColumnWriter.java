package org.embulk.input.web_api.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public interface ColumnWriter
{
    Column getColumn();

    void write(JsonNode v, PageBuilder to);
}
