package org.embulk.base.restclient.jackson.scope;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.embulk.base.restclient.record.SinglePageRecordReader;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;

public class JacksonAllInObjectScope
        extends JacksonObjectScopeBase
{
    public JacksonAllInObjectScope()
    {
        this(null);
    }

    public JacksonAllInObjectScope(String timestampFormat)
    {
        this.timestampFormat = timestampFormat;
    }

    @Override
    public ObjectNode scopeObject(final SinglePageRecordReader singlePageRecordReader)
    {
        final ObjectNode resultObject = JsonNodeFactory.instance.objectNode();

        singlePageRecordReader.getSchema().visitColumns(new ColumnVisitor()
            {
                @Override
                public void booleanColumn(Column column)
                {
                    resultObject.put(column.getName(),
                                     BooleanNode.valueOf(singlePageRecordReader.getBoolean(column)));
                }

                @Override
                public void longColumn(Column column)
                {
                    resultObject.put(column.getName(),
                                     new LongNode(singlePageRecordReader.getLong(column)));
                }

                @Override
                public void doubleColumn(Column column)
                {
                    resultObject.put(column.getName(),
                                     new DoubleNode(singlePageRecordReader.getDouble(column)));
                }

                @Override
                public void stringColumn(Column column)
                {
                    resultObject.put(column.getName(),
                                     new TextNode(singlePageRecordReader.getString(column)));
                }

                @Override
                public void timestampColumn(Column column)
                {
                    if (timestampFormat == null) {
                        resultObject.put(column.getName(),
                                         new LongNode(singlePageRecordReader.getTimestamp(column).getEpochSecond()));
                    }
                    else {
                        // TODO(dmikurube): Implement timestamp formatting.
                    }
                }

                @Override
                public void jsonColumn(Column column)
                {
                    // TODO(dmikurube): Implement conversion from msgpack |Value| to Jackson |JsonNode|.
                }
            });
        return resultObject;
    }

    private final String timestampFormat;
}
