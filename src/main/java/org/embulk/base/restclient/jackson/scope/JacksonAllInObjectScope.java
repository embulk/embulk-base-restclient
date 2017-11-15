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
        this(null, false);
    }

    public JacksonAllInObjectScope(boolean fillsJsonNullForEmbulkNull)
    {
        this(null, fillsJsonNullForEmbulkNull);
    }

    public JacksonAllInObjectScope(final TimestampFormatter timestampFormatter)
    {
        this(timestampFormatter, false);
    }

    public JacksonAllInObjectScope(final TimestampFormatter timestampFormatter, boolean fillsJsonNullForEmbulkNull)
    {
        this.timestampFormatter = timestampFormatter;
        this.jsonParser = new StringJsonParser();
        this.fillsJsonNullForEmbulkNull = fillsJsonNullForEmbulkNull;
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
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(),
                                singlePageRecordReader.getBoolean(column));
                    } else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void longColumn(Column column)
                {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(),
                                singlePageRecordReader.getLong(column));
                    } else if (fillsJsonNullForEmbulkNull)  {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void doubleColumn(Column column)
                {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(),
                                singlePageRecordReader.getDouble(column));
                    } else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void stringColumn(Column column)
                {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(),
                                         singlePageRecordReader.getString(column));
                    } else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
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
                    } else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
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
                    } else {
                        resultObject.putNull(column.getName());
                    }
                }
            });
        return resultObject;
    }

    private final TimestampFormatter timestampFormatter;
    private final StringJsonParser jsonParser;
    private final boolean fillsJsonNullForEmbulkNull;
}
