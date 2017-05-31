package org.embulk.base.restclient.jackson.scope;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.time.TimestampFormatter;

import org.embulk.base.restclient.jackson.StringJsonParser;
import org.embulk.base.restclient.record.SinglePageRecordReader;

public class JacksonAllInObjectScope
        extends JacksonObjectScopeBase
{
    public JacksonAllInObjectScope()
    {
        this(null);
    }

    public JacksonAllInObjectScope(final TimestampFormatter timestampFormatter)
    {
        this.timestampFormatter = timestampFormatter;
        this.jsonParser = new StringJsonParser();
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
                                     singlePageRecordReader.getBoolean(column));
                }

                @Override
                public void longColumn(Column column)
                {
                    resultObject.put(column.getName(),
                                     singlePageRecordReader.getLong(column));
                }

                @Override
                public void doubleColumn(Column column)
                {
                    resultObject.put(column.getName(),
                                     singlePageRecordReader.getDouble(column));
                }

                @Override
                public void stringColumn(Column column)
                {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(),
                                         singlePageRecordReader.getString(column));
                    }
                }

                @Override
                public void timestampColumn(Column column)
                {
                    if (!singlePageRecordReader.isNull(column)) {
                        if (timestampFormatter == null) {
                            resultObject.put(column.getName(),
                                             singlePageRecordReader.getTimestamp(column).getEpochSecond());
                        } else {
                            resultObject.put(column.getName(),
                                             timestampFormatter.format(singlePageRecordReader.getTimestamp(column)));
                        }
                    }
                }

                @Override
                public void jsonColumn(Column column)
                {
                    // TODO(dmikurube): Use jackson-datatype-msgpack.
                    // See: https://github.com/embulk/embulk-base-restclient/issues/32
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.set(column.getName(),
                                jsonParser.parseJsonObject(singlePageRecordReader.getJson(column).toJson()));
                    }
                }
            });
        return resultObject;
    }

    private final TimestampFormatter timestampFormatter;
    private final StringJsonParser jsonParser;
}
