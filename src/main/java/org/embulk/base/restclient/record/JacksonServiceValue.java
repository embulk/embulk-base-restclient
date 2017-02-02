package org.embulk.base.restclient.record;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * JacksonServiceValue represents a value in a JSON response to be converted to an Embulk column value.
 *
 * {@code JacksonServiceValue} depends on Jackson {@code JsonNode}'s {@code as*} methods if type
 * conversion is needed.
 *
 * For example with Jackson 2.5.0, the JSON below is recognized as {@code boolean} {@code true}.
 * <pre>{@code
 * { "flag": "true" }
 * }</pre>
 *
 * The other JSON below however cannot be recognized as Embulk's {@code boolean} {@code true}
 * because Jackson 2.5.0 recognizes only {@code "true"} and {@code "false"}.
 * <pre>{@code
 * { "flag": "True" }
 * }</pre>
 *
 * @see https://github.com/FasterXML/jackson-databind/blob/jackson-databind-2.5.0/src/main/java/com/fasterxml/jackson/databind/node/TextNode.java#L177-L189
 *
 * Implement another set of {@code ServiceValue} and {@code ServiceRecord} if a different style of
 * type conversion is required.
 */
public class JacksonServiceValue
        extends ServiceValue
{
    public JacksonServiceValue(JsonNode value)
    {
        this.value = value;
    }

    @Override
    public boolean isNull()
    {
        return value.isNull();
    }

    @Override
    public boolean booleanValue()
    {
        return value.asBoolean();
    }

    @Override
    public double doubleValue()
    {
        return value.asDouble();
    }

    @Override
    public org.msgpack.value.Value jsonValue(org.embulk.spi.json.JsonParser jsonParser)
    {
        return jsonParser.parse(value.asText());
    }

    @Override
    public long longValue()
    {
        return value.asLong();
    }

    @Override
    public String stringValue()
    {
        return value.asText();
    }

    @Override
    public org.embulk.spi.time.Timestamp timestampValue(org.embulk.spi.time.TimestampParser timestampParser)
    {
        return timestampParser.parse(value.asText());
    }

    private JsonNode value;
}
