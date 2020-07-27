package org.embulk.base.restclient.jackson.scope;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.base.restclient.jackson.StringJsonParser;
import org.embulk.base.restclient.record.SinglePageRecordReader;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.time.TimestampFormatter;

public class JacksonAllInObjectScope extends JacksonObjectScopeBase {
    public JacksonAllInObjectScope() {
        this(null, false);
    }

    public JacksonAllInObjectScope(final boolean fillsJsonNullForEmbulkNull) {
        this(null, fillsJsonNullForEmbulkNull);
    }

    public JacksonAllInObjectScope(final TimestampFormatter timestampFormatter) {
        this(timestampFormatter, false);
    }

    public JacksonAllInObjectScope(final TimestampFormatter timestampFormatter, final boolean fillsJsonNullForEmbulkNull) {
        this.timestampFormatter = timestampFormatter;
        this.jsonParser = new StringJsonParser();
        this.fillsJsonNullForEmbulkNull = fillsJsonNullForEmbulkNull;
    }

    @Override
    public ObjectNode scopeObject(final SinglePageRecordReader singlePageRecordReader) {
        final ObjectNode resultObject = JsonNodeFactory.instance.objectNode();

        singlePageRecordReader.getSchema().visitColumns(new ColumnVisitor() {
                @Override
                public void booleanColumn(final Column column) {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(), singlePageRecordReader.getBoolean(column));
                    } else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void longColumn(final Column column) {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(), singlePageRecordReader.getLong(column));
                    } else if (fillsJsonNullForEmbulkNull)  {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void doubleColumn(final Column column) {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(), singlePageRecordReader.getDouble(column));
                    } else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void stringColumn(final Column column) {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(), singlePageRecordReader.getString(column));
                    } else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void timestampColumn(final Column column) {
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
                public void jsonColumn(final Column column) {
                    // TODO(dmikurube): Use jackson-datatype-msgpack.
                    // See: https://github.com/embulk/embulk-base-restclient/issues/32
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.set(
                                column.getName(),
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
