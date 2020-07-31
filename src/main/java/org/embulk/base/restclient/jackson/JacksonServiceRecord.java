package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.base.restclient.record.ValueLocator;

public class JacksonServiceRecord extends ServiceRecord {
    public JacksonServiceRecord(final ObjectNode record) {
        this.record = record;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends ServiceRecord.Builder {
        public Builder() {
            this.node = JsonNodeFactory.instance.objectNode();
        }

        @Override
        public void reset() {
            this.node = JsonNodeFactory.instance.objectNode();
        }

        @Override
        public void add(final ServiceValue serviceValue, final ValueLocator valueLocator) {
            final JacksonServiceValue jacksonServiceValue;
            try {
                jacksonServiceValue = (JacksonServiceValue) serviceValue;
            } catch (final ClassCastException ex) {
                throw new RuntimeException("Non-JacksonServiceValue is given to place in JacksonServiceRecord:", ex);
            }

            final JacksonValueLocator jacksonValueLocator;
            try {
                jacksonValueLocator = (JacksonValueLocator) valueLocator;
            } catch (final ClassCastException ex) {
                throw new RuntimeException("Non-JacksonValueLocator is used to place a value in JacksonServiceRecord:", ex);
            }

            jacksonValueLocator.placeValue(this.node, jacksonServiceValue.getInternalJsonNode());
        }

        @Override
        public ServiceRecord build() {
            final JacksonServiceRecord built = new JacksonServiceRecord(this.node);
            this.reset();
            return built;
        }

        private ObjectNode node;
    }

    @Override
    public JacksonServiceValue getValue(final ValueLocator locator) {
        // Based on the implementation of |JacksonServiceResponseSchema|,
        // the |ValueLocator| should be always |JacksonValueLocator|.
        // The class cast should always success.
        //
        // NOTE: Just casting (with catching |ClassCastException|) is
        // faster than checking (instanceof) and then casting if
        // the |ClassCastException| never or hardly happens.
        final JacksonValueLocator jacksonLocator;
        try {
            jacksonLocator = (JacksonValueLocator) locator;
        } catch (final ClassCastException ex) {
            throw new RuntimeException("Non-JacksonValueLocator is used to locate a value in JacksonServiceRecord", ex);
        }
        return new JacksonServiceValue(jacksonLocator.seekValue(record));
    }

    @Override
    public String toString() {
        return this.record.toString();
    }

    ObjectNode getInternalJsonNode() {
        return this.record;
    }

    private final ObjectNode record;
}
